"""Change data capture from PostgreSQL into the bronze Delta layer.

The job reads the 6 operational tables through JDBC and merges the changed rows
into `walmart.bronze.*`. It runs in Databricks. The Airflow task `ingest_cdc`
starts it by job ID and waits for the result.

How the job decides what to read
--------------------------------
Each source table has an `updated_timestamp` column. The job reads the maximum
value already in the bronze table and asks PostgreSQL only for the rows that are
newer. On the first run the bronze table does not exist, so the job reads the
full table and creates it.

Credentials
-----------
The job reads 3 values from the Databricks secret scope named by SECRET_SCOPE.
Create the scope one time:

    databricks secrets create-scope walmart
    databricks secrets put-secret walmart pg-jdbc-url
    databricks secrets put-secret walmart pg-user
    databricks secrets put-secret walmart pg-password

The JDBC URL has this shape:

    jdbc:postgresql://host:5432/dbname?sslmode=require

Outside Databricks the job falls back to the environment variables PG_JDBC_URL,
PG_USER and PG_PASSWORD, which makes local testing possible.

Cluster requirement
-------------------
The cluster needs the PostgreSQL JDBC driver. Install the Maven coordinate
`org.postgresql:postgresql:42.7.4` on the cluster, or attach the JAR.
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

CATALOG = "walmart"
BRONZE_SCHEMA = "bronze"
SOURCE_SCHEMA = "raw"
SECRET_SCOPE = "walmart"

# Source table to primary key. The merge condition uses the primary key.
TABLES = {
    "customers": "customer_id",
    "stores": "store_id",
    "products": "product_id",
    "employees": "employee_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}

# Every source table carries this column. It drives both the incremental read
# here and the incremental models in 03_transform/models/silver_t.
WATERMARK_COLUMN = "updated_timestamp"

JDBC_DRIVER = "org.postgresql.Driver"


def get_spark() -> SparkSession:
    """Return the active session, or build one for a Python script task."""
    return SparkSession.builder.appName("walmart_cdc_bronze").getOrCreate()


def get_secret(spark: SparkSession, key: str, env_var: str) -> str:
    """Read a secret from the Databricks scope, or from the environment."""
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark).secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception:
        value = os.environ.get(env_var)
        if not value:
            raise ValueError(
                f"No secret '{key}' in scope '{SECRET_SCOPE}' and no {env_var} "
                f"environment variable. See the docstring in this file."
            )
        return value


def get_watermark(spark: SparkSession, target: str) -> str:
    """Return the newest updated_timestamp in the bronze table, or None."""
    if not spark.catalog.tableExists(target):
        return None

    row = spark.sql(f"SELECT MAX({WATERMARK_COLUMN}) AS high_water FROM {target}").first()
    if row is None or row["high_water"] is None:
        return None
    return str(row["high_water"])


def read_source(
    spark: SparkSession,
    table: str,
    since: str,
    jdbc_url: str,
    user: str,
    password: str,
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

    The source primary key guarantees one row per key, so the merge never sees
    two source rows for the same target row.
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

    jdbc_url = get_secret(spark, "pg-jdbc-url", "PG_JDBC_URL")
    user = get_secret(spark, "pg-user", "PG_USER")
    password = get_secret(spark, "pg-password", "PG_PASSWORD")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

    total = 0
    for table, primary_key in TABLES.items():
        target = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
        since = get_watermark(spark, target)

        frame = read_source(spark, table, since, jdbc_url, user, password)
        row_count = frame.count()

        mode = "full load" if since is None else f"changes after {since}"
        print(f"{table}: {mode}, {row_count} rows")

        if row_count == 0:
            continue

        write_bronze(spark, frame, target, primary_key)
        total += row_count

    print(f"\nDone. {total} rows written to {CATALOG}.{BRONZE_SCHEMA}.")


if __name__ == "__main__":
    main()
