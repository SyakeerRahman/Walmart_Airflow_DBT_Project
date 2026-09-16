"""Change data capture into the bronze Delta layer.

The job loads the 6 operational tables and merges the changed rows into
`walmart.bronze.*`. It runs in Databricks. The Airflow task `ingest_cdc` starts
it by job ID and waits for the result.

Two source modes
----------------
`volume`  Reads the CSV files from a Unity Catalog volume. This mode works on
          Databricks Free Edition, where compute is serverless and cannot install
          a JDBC driver or open a connection to an external database.

`jdbc`    Reads the tables directly from PostgreSQL. This mode needs a classic
          cluster with the Maven library `org.postgresql:postgresql:42.7.4` and
          network access to the database host.

Set the mode with a job parameter named `source_mode`, or with the SOURCE_MODE
environment variable. The default is `volume`.

How the job decides what to read
--------------------------------
Each source table has an `updated_timestamp` column. The job reads the maximum
value already in the bronze table and keeps only the rows that are newer. On the
first run the bronze table does not exist, so the job loads everything and
creates it. Both modes share this logic, so both produce the same bronze layer.

Credentials for jdbc mode
-------------------------
The job reads 3 values from the Databricks secret scope named by SECRET_SCOPE:

    databricks secrets create-scope walmart
    databricks secrets put-secret walmart pg-jdbc-url
    databricks secrets put-secret walmart pg-user
    databricks secrets put-secret walmart pg-password

The JDBC URL has this shape:

    jdbc:postgresql://host:5432/dbname?sslmode=require

`volume` mode needs no credentials.
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

CATALOG = "walmart"
BRONZE_SCHEMA = "bronze"
SOURCE_SCHEMA = "raw"
SECRET_SCOPE = "walmart"

# Where volume mode looks for the CSV files.
VOLUME_NAME = "landing"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"

# Source table to primary key. The merge condition uses the primary key.
TABLES = {
    "customers": "customer_id",
    "stores": "store_id",
    "products": "product_id",
    "employees": "employee_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}

# Column types, in Spark DDL form. They mirror 01_source/ddl/walmart_schema.sql.
# Volume mode needs them, because type inference on a CSV file is not reliable.
SCHEMAS = {
    "customers": (
        "customer_id BIGINT, first_name STRING, last_name STRING, email STRING, "
        "phone STRING, city STRING, province STRING, country STRING, "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, is_active STRING"
    ),
    "stores": (
        "store_id BIGINT, store_name STRING, city STRING, province STRING, country STRING, "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, is_active STRING"
    ),
    "products": (
        "product_id BIGINT, product_name STRING, category STRING, brand STRING, "
        "price DECIMAL(10,2), "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, is_active STRING"
    ),
    "employees": (
        "employee_id BIGINT, store_id BIGINT, first_name STRING, last_name STRING, "
        "email STRING, job_title STRING, salary DECIMAL(10,2), "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, is_active STRING"
    ),
    "orders": (
        "order_id BIGINT, customer_id BIGINT, store_id BIGINT, order_timestamp TIMESTAMP, "
        "payment_method STRING, order_status STRING, total_amount DECIMAL(12,2), "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, is_active STRING"
    ),
    "order_items": (
        "order_item_id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, "
        "unit_price DECIMAL(10,2), line_amount DECIMAL(12,2), "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, is_active STRING"
    ),
}

# Every source table carries this column. It drives both the incremental read
# here and the incremental models in 03_transform/models/silver_t.
WATERMARK_COLUMN = "updated_timestamp"

CSV_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
JDBC_DRIVER = "org.postgresql.Driver"


def get_spark() -> SparkSession:
    """Return the active session, or build one for a Python script task."""
    return SparkSession.builder.appName("walmart_cdc_bronze").getOrCreate()


def get_dbutils(spark: SparkSession):
    """Return DBUtils, or None when the job runs outside Databricks."""
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except Exception:
        return None


def get_source_mode(spark: SparkSession) -> str:
    """Read the source mode from the job parameter, or from the environment."""
    dbutils = get_dbutils(spark)
    if dbutils is not None:
        try:
            value = dbutils.widgets.get("source_mode")
            if value:
                return value.strip().lower()
        except Exception:
            pass

    mode = os.environ.get("SOURCE_MODE", "volume").strip().lower()
    if mode not in ("volume", "jdbc"):
        raise ValueError(f"source_mode must be 'volume' or 'jdbc', not '{mode}'")
    return mode


def get_secret(spark: SparkSession, key: str, env_var: str) -> str:
    """Read a secret from the Databricks scope, or from the environment."""
    dbutils = get_dbutils(spark)
    if dbutils is not None:
        try:
            return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)
        except Exception:
            pass

    value = os.environ.get(env_var)
    if not value:
        raise ValueError(
            f"No secret '{key}' in scope '{SECRET_SCOPE}' and no {env_var} "
            f"environment variable. See the docstring in this file."
        )
    return value


def get_watermark(spark: SparkSession, target: str):
    """Return the newest updated_timestamp in the bronze table, or None."""
    if not spark.catalog.tableExists(target):
        return None

    row = spark.sql(f"SELECT MAX({WATERMARK_COLUMN}) AS high_water FROM {target}").first()
    if row is None or row["high_water"] is None:
        return None
    return str(row["high_water"])


def read_from_volume(spark: SparkSession, table: str, since) -> DataFrame:
    """Read one CSV file from the Unity Catalog volume and apply the watermark."""
    frame = (
        spark.read.schema(SCHEMAS[table])
        .option("header", "true")
        .option("timestampFormat", CSV_TIMESTAMP_FORMAT)
        .csv(f"{VOLUME_PATH}/{table}.csv")
    )

    if since is not None:
        frame = frame.where(f"{WATERMARK_COLUMN} > TIMESTAMP '{since}'")
    return frame


def read_from_jdbc(
    spark: SparkSession, table: str, since, jdbc_url: str, user: str, password: str
) -> DataFrame:
    """Read the rows of one source table that changed after `since`."""
    query = f"SELECT * FROM {SOURCE_SCHEMA}.{table}"
    if since is not None:
        # `since` comes from our own bronze table, never from user input.
        query += f" WHERE {WATERMARK_COLUMN} > TIMESTAMP '{since}'"

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", query)
        .option("user", user)
        .option("password", password)
        .option("driver", JDBC_DRIVER)
        .load()
    )


def write_bronze(spark: SparkSession, frame: DataFrame, target: str, primary_key: str) -> None:
    """Create the bronze table, or merge the changed rows into it.

    The source primary key gives one row per key, so the merge never sees two
    source rows for the same target row.
    """
    if not spark.catalog.tableExists(target):
        frame.write.format("delta").saveAsTable(target)
        return

    (
        DeltaTable.forName(spark, target)
        .alias("target")
        .merge(frame.alias("source"), f"target.{primary_key} = source.{primary_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    spark = get_spark()
    mode = get_source_mode(spark)
    print(f"Source mode: {mode}")

    jdbc_url = user = password = None
    if mode == "jdbc":
        jdbc_url = get_secret(spark, "pg-jdbc-url", "PG_JDBC_URL")
        user = get_secret(spark, "pg-user", "PG_USER")
        password = get_secret(spark, "pg-password", "PG_PASSWORD")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
    if mode == "volume":
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{VOLUME_NAME}")

    total = 0
    for table, primary_key in TABLES.items():
        target = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
        since = get_watermark(spark, target)

        if mode == "volume":
            frame = read_from_volume(spark, table, since)
        else:
            frame = read_from_jdbc(spark, table, since, jdbc_url, user, password)

        row_count = frame.count()
        window = "full load" if since is None else f"changes after {since}"
        print(f"{table}: {window}, {row_count} rows")

        if row_count == 0:
            continue

        write_bronze(spark, frame, target, primary_key)
        total += row_count

    print(f"\nDone. {total} rows written to {CATALOG}.{BRONZE_SCHEMA}.")


if __name__ == "__main__":
    main()
