# Walmart Data Engineering Project

![Walmart Data Engineering with Airflow and dbt](docs/images/banner.png)

An end-to-end lakehouse pipeline. PostgreSQL holds the operational data. A Databricks job moves
changed rows into a bronze Delta layer. dbt builds the silver and gold layers. Apache Airflow
runs the steps in order.

The guide below takes you from an empty database to a verified gold layer in 11 steps.

## Architecture

```
PostgreSQL (raw schema)  or  CSV files in a Unity Catalog volume
        |  CDC job (PySpark, runs in Databricks)
        v
walmart.bronze.*            6 Delta tables
        |  dbt run --select silver_t
        v
walmart.silver_t.*          6 incremental tables, one per source table
        |  dbt run --select silver_b
        v
walmart.silver_b.obt_b      1 wide table (One Big Table)
        |  dbt run + dbt snapshot
        v
walmart.gold.*              5 SCD2 dimensions + 1 fact table
```

Apache Airflow triggers each step. The Airflow stack runs in Docker on your machine. Databricks
does the compute.

## Repository layout

The directories follow the order of the pipeline.

```
01_source/           The operational PostgreSQL database
  ddl/               Table definitions
  data/              6 sample CSV files
  load_data.py       Bulk loader

02_ingestion/        The Databricks CDC job
  cdc_bronze.py      CSV volume or PostgreSQL -> bronze Delta, merge on the PK

03_transform/        The dbt project
  models/source/     Bronze source declarations
  models/silver_t/   6 incremental technical models
  models/silver_b/   The One Big Table
  models/gold/       Ephemeral models and the fact table
  snapshots/         5 SCD2 dimensions
  macros/            Schema name override
  tests/             Singular tests

04_orchestration/    The Airflow stack
  docker-compose.yaml
  Dockerfile
  requirements.txt
  config/
  dags/              The orchestrate DAG

docs/images/         Diagrams and the banner
```

Copy `.env.example` to `04_orchestration/.env` before the first run. The real `.env` stays out
of Git.

## What you need first

| Item | Purpose |
|---|---|
| Docker Desktop, 4 GB of memory or more | It runs the Airflow stack. |
| A Databricks workspace with Unity Catalog | It runs the CDC job and the dbt models. |
| A PostgreSQL database that Databricks can reach | Only for JDBC mode. See Step 4. |
| Python 3.9 or later, with `psycopg2` | Only for JDBC mode. It loads the sample data. |
| Git | It clones this repository. |

Databricks Free Edition is enough. It gives you serverless compute only, so follow volume mode
in Step 4 and skip Step 1 and Step 2. PostgreSQL is needed only for JDBC mode, which needs a
classic cluster.

---

## Step 1 - Create the source database

**Skip this step in volume mode.** Volume mode reads the CSV files directly.

1. Create an empty PostgreSQL database.
2. Create the schema:

   ```sql
   CREATE SCHEMA raw;
   ```

3. Run [01_source/ddl/walmart_schema.sql](01_source/ddl/walmart_schema.sql) in that
   schema. Set `search_path` to `raw` first, because the DDL file does not qualify the table
   names.

The DDL file creates 6 tables: `customers`, `stores`, `products`, `employees`, `orders`, and
`order_items`. Every table has a `created_timestamp`, an `updated_timestamp`, and an `is_active`
column. The CDC job and the dbt incremental models both use `updated_timestamp`.

## Step 2 - Load the sample data

**Skip this step in volume mode.**

[01_source/load_data.py](01_source/load_data.py) reads the connection string from an environment
variable. It holds no credentials.

1. Install the driver:

   ```powershell
   pip install psycopg2-binary
   ```

2. Set the connection string and run the loader:

   ```powershell
   $env:POSTGRES_CONN_STRING = "postgresql://user:pass@host:5432/dbname"
   python 01_source\load_data.py
   ```

The script loads 6 CSV files with the `COPY` command. Expect 2000 customers, 25 stores, 500
products, 250 employees, 10000 orders, and 30021 order items. It stops with a clear message if
the variable is empty.

## Step 3 - Prepare Databricks

1. Create a Unity Catalog catalog with the name `walmart`.
2. Create 4 schemas in it: `bronze`, `silver_t`, `silver_b`, and `gold`.
3. Create a SQL warehouse. Copy its **Server hostname** and its **HTTP path**.
4. Create a personal access token. Copy the token value now, because Databricks shows it one time.

The schema names are not free choices. [dbt_project.yml](03_transform/dbt_project.yml)
and the snapshot files write these exact names.

## Step 4 - Set up the CDC ingestion job

[02_ingestion/cdc_bronze.py](02_ingestion/cdc_bronze.py) is the PySpark job. It fills the bronze
layer. The Airflow DAG starts it by job ID and then waits for the result.

For each of the 6 source tables the job does this:

1. Read the maximum `updated_timestamp` that is already in the bronze table.
2. Keep only the source rows that are newer than that value.
3. Merge those rows into `walmart.bronze.<table>` on the primary key. The merge updates the old
   rows and inserts the new rows.
4. On the first run the bronze table does not exist, so the job loads everything and creates the
   table.

Follow **4a** or **4b**, then do **4c**. Both modes produce the same bronze layer.

### 4a - Volume mode, for Databricks Free Edition

Free Edition gives you serverless compute only. Serverless cannot install a JDBC driver and
cannot open a connection to an external database, so the job reads the CSV files instead.
PostgreSQL is not needed for this mode, and you can skip Step 1 and Step 2.

1. Create the volume:

   ```sql
   CREATE VOLUME IF NOT EXISTS walmart.bronze.landing;
   ```

2. Upload the 6 files from [01_source/data/](01_source/data/) to
   `/Volumes/walmart/bronze/landing/`. Use **Catalog -> walmart -> bronze -> landing -> Upload**.
   Keep the file names.
3. Add a job parameter named `source_mode` with the value `volume`.

### 4b - JDBC mode, for a paid workspace

This mode reads PostgreSQL directly and needs a classic cluster.

1. Store the 3 secrets:

   ```bash
   databricks secrets create-scope walmart
   databricks secrets put-secret walmart pg-jdbc-url
   databricks secrets put-secret walmart pg-user
   databricks secrets put-secret walmart pg-password
   ```

   The JDBC URL has this shape: `jdbc:postgresql://host:5432/dbname?sslmode=require`

2. Install `org.postgresql:postgresql:42.7.4` from Maven on the cluster.
3. Give the cluster network access to your PostgreSQL host.
4. Add a job parameter named `source_mode` with the value `jdbc`.

### 4c - Create and run the job

1. Upload `cdc_bronze.py` to your workspace, or connect the workspace to this Git repository.
2. Create a job with one task that points at the file.
3. Run the job one time by hand. The output prints the mode and a row count for each table.
4. Copy the job ID from the job page URL. Step 9 puts it in `.env`.

Check the result with `SELECT COUNT(*) FROM walmart.bronze.orders`. Expect 10000 rows.

## Step 5 - Create the dbt project

The finished dbt project is in
[03_transform/](03_transform/). To build it from
nothing, run `dbt init walmart_project` and then add the files below.

### 5a - Connection profile

Put [profiles.yml](03_transform/profiles.yml) **inside** the project
directory, not in `~/.dbt`. dbt reads the working directory, so every command in this project
works without the `--profiles-dir` flag.

The file holds no credentials. The `env_var()` function reads them at run time:

```yaml
walmart_project:
  outputs:
    dev:
      type: databricks
      catalog: walmart
      schema: dbt_schema
      threads: 1
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
  target: dev
```

You set these 3 variables one time, in `04_orchestration/.env`, in Step 9. dbt stops with a
clear message if a variable is missing.

### 5b - Schema routing

[dbt_project.yml](03_transform/dbt_project.yml) sends each model directory
to its own schema:

```yaml
models:
  walmart_project:
    silver_t:
      +materialized: table
      +schema: silver_t
    silver_b:
      +materialized: table
      +schema: silver_b
    gold:
      +materialized: table
      +schema: gold
      ephemeral:
        +materialized: ephemeral
```

By default, dbt puts a prefix on a custom schema name. The result is `dbt_schema_silver_t`. The
macro [macros/custom_schema.sql](03_transform/macros/custom_schema.sql)
overrides that behavior. It returns the custom name without a change, so the models land in
`walmart.silver_t` and not in `walmart.dbt_schema_silver_t`. Add this macro before you run any
model.

### 5c - Sources

[models/source/sources.yml](03_transform/models/source/sources.yml) points
dbt at the 6 bronze tables from Step 4. It also declares freshness:

```yaml
    loaded_at_field: updated_timestamp
    freshness:
      warn_after: {count: 24, period: hour}
```

The CDC job carries `updated_timestamp` over from PostgreSQL, so that column shows the age of
the newest row. There is no `error_after` threshold. The sample data has fixed timestamps, and
an error threshold would stop every run.

## Step 6 - Build the silver technical layer

Add one model for each source table in `models/silver_t/`. Each model is short, and each model
follows the same pattern:

```sql
{{ config(materialized='incremental', unique_key='customer_id') }}

SELECT *, current_timestamp() AS processed_at
FROM {{ source('walmart_databricks', 'customers') }}

{% if is_incremental() %}
    WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
```

Rules for this layer:

- Do not reshape the data. `SELECT *` is correct here.
- Add only the `processed_at` column.
- Set `unique_key` to the primary key of the table.
- The `is_incremental()` block makes the first run a full load and every later run a delta load.

Add the data tests in
[models/silver_t/properties.yml](03_transform/models/silver_t/properties.yml).
The file tests `product_id` and `order_id` for `not_null` and `unique`.

## Step 7 - Build the One Big Table

[models/silver_b/obt_b.sql](03_transform/models/silver_b/obt_b.sql) joins
the 6 silver tables into 1 wide table. The model is metadata driven. A Jinja list holds one
dictionary for each table:

```jinja
{% set configs = [
    { "table": "walmart.silver_t.orders_t", "columns": "...", "alias": "o" },
    { "table": "walmart.silver_t.customers_t", "columns": "...", "alias": "c",
      "join_condition": "o.customer_id = c.customer_id" }
] %}
```

One loop then writes the `SELECT` list. A second loop writes the `FROM` clause, with a
`LEFT JOIN` for each table after the first one.

- The first dictionary in the list is the driving table. It has no `join_condition`.
- To add a column, edit the `columns` string in the dictionary. Do not edit the SQL below the
  list.
- The column names get a prefix for each entity, for example `customer_city` and `store_city`.
  This step prevents a name conflict in the wide table.

**Important:** this model writes the full table names, such as `walmart.silver_t.orders_t`. It
does not use `ref()`. For this reason, dbt does not know that the silver technical layer comes
first. The Airflow DAG controls the order instead.

## Step 8 - Build the gold layer

The gold layer has 3 parts.

### 8a - Ephemeral models

Add 5 models in `models/gold/ephemeral/`, one for each entity. Each model does a
`SELECT DISTINCT` on `ref('obt_b')`:

```sql
SELECT DISTINCT
    customer_id,
    customer_first_name,
    CURRENT_TIMESTAMP() AS customer_gold_processed_at
FROM {{ ref('obt_b') }}
```

These models are ephemeral. dbt does not write them to the warehouse. dbt puts their SQL into the
model that comes next.

### 8b - Snapshots

Add 5 YAML files in `snapshots/`. Each file turns one ephemeral model into a slowly changing
dimension of type 2:

```yaml
snapshots:
  - name: dim_customers
    relation: ref('eph_customers')
    config:
      schema: gold
      database: walmart
      unique_key: customer_id
      strategy: timestamp
      updated_at: customer_updated_timestamp
      dbt_valid_to_current: "to_date('9999-12-31')"
```

The `timestamp` strategy compares `updated_at` with the stored value. When the value changes, dbt
closes the old row and writes a new row. The `dbt_valid_to_current` setting puts `9999-12-31` in
the current row instead of `NULL`. A date filter is then easier to write.

This YAML format needs dbt 1.9 or later.

### 8c - Fact table

[models/gold/fact/fact_orders.sql](03_transform/models/gold/fact/fact_orders.sql)
holds the 6 keys and the 4 measures from `ref('obt_b')`.

Add the singular test
[tests/test_obt.sql](03_transform/tests/test_obt.sql). The test finds a row
with a `NULL` key. Its severity is `warn`, so a problem does not stop the pipeline.

## Step 9 - Set up Airflow in Docker

Work in the `04_orchestration` directory.

1. Get the official Compose file:

   ```powershell
   curl -o docker-compose.yaml https://airflow.apache.org/docs/apache-airflow/3.2.2/docker-compose.yaml
   ```

2. In that file, replace the `image:` line with `build: .` and add the dbt project as a volume:

   ```yaml
     volumes:
       - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
       - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
       - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
       - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
       - ../03_transform:/opt/airflow/dbt
   ```

   The last line is the important one. The dbt project lives one directory up, and the
   containers must see it at `/opt/airflow/dbt`.

3. Write [requirements.txt](04_orchestration/requirements.txt):

   ```
   airflow-operators>=0.11.0
   apache-airflow>=3.2.2
   dbt-core>=1.11.11
   dbt-databricks>=1.12.1
   ```

   Save this file as UTF-8. `pip` can fail to read a UTF-16 requirements file during the build.

4. Write [Dockerfile](04_orchestration/Dockerfile). It extends the Airflow image and installs
   the requirements:

   ```dockerfile
   FROM apache/airflow:3.2.0
   USER root
   RUN apt-get update && apt-get install -y gcc && apt-get clean
   USER airflow
   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt
   ```

   The Databricks SDK arrives with `dbt-databricks`, so the DAG can import it.

5. Copy the template and fill it in. This file holds every credential in the project:

   ```powershell
   copy ..\.env.example .env
   ```

   ```
   AIRFLOW_UID=50000
   FERNET_KEY=<generated key>
   DATABRICKS_HOST=<from Step 3>
   DATABRICKS_TOKEN=<from Step 3>
   DATABRICKS_HTTP_PATH=<from Step 3>
   DATABRICKS_JOB_ID=<from Step 4>
   ```

   Generate the Fernet key with this command:

   ```powershell
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

   `.env` is in `.gitignore`. Compose loads it into every Airflow container, so the DAG and dbt
   both read the same values. Never commit this file.

6. Build the image and start the stack:

   ```powershell
   docker compose build
   docker compose up -d
   ```

7. Open http://localhost:8080. The user name is `airflow` and the password is `airflow`.

## Step 10 - Write the DAG

[dags/orchestrate.py](04_orchestration/dags/orchestrate.py) holds 1 DAG with 10 tasks in a
straight line:

```
ingest_cdc -> clean_target -> source_freshness
           -> silver_technical -> silver_technical_tests
           -> silver_business  -> silver_business_tests
           -> gold_ephemeral   -> gold_dimensions -> gold_facts
```

Two task types do the work:

- `ingest_cdc` is a Python task. It calls `WorkspaceClient.jobs.run_now()` with your Databricks
  job ID. Then it polls `jobs.get_run()` every 5 seconds. It stops the DAG when the job result is
  not `SUCCESS`.
- Every dbt step is a `BashOperator` with `cwd='/opt/airflow/dbt'`. The `cwd`
  parameter puts dbt in the project directory, so dbt finds `profiles.yml` there.

The DAG holds no credentials. `WorkspaceClient()` takes `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
from the container environment, and the task reads `DATABRICKS_JOB_ID` the same way. The task
fails with a clear message if the job ID is missing.

The `clean_target` task removes the `target` and `logs` directories before each run. This step
stops dbt from the use of an old manifest.

## Step 11 - Run the pipeline

1. Open the Airflow UI.
2. Turn on the `orchestrate` DAG.
3. Start a manual run.
4. Watch the Graph view. The tasks run one after the other.

A full run takes 10 to 20 minutes on a small SQL warehouse. Most of that time is the warehouse
start time.

### Verify the result

Query these tables in Databricks:

| Table | Expected result |
|---|---|
| `walmart.bronze.orders` | 10000 rows |
| `walmart.silver_t.orders_t` | 10000 rows, plus a `processed_at` column |
| `walmart.silver_b.obt_b` | 30021 rows, one row for each order item |
| `walmart.gold.dim_customers` | 2000 rows, plus the `dbt_valid_from` and `dbt_valid_to` columns |
| `walmart.gold.fact_orders` | 30021 rows |

To test the incremental behavior, update some rows in PostgreSQL, set a new `updated_timestamp`,
and start the DAG again. Only the changed rows move through the pipeline. The dimensions get a
new version of each changed row.

## Run one step by hand

The dbt commands run inside the containers. Use this form:

```powershell
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt run --select silver_t"
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt test --select products_t"
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt snapshot"
```

## Known limitations

- JDBC mode needs a classic cluster that can install a Maven library and reach your PostgreSQL
  host. Serverless compute allows neither. Volume mode exists for that reason.
- Volume mode reads a static CSV file, so it shows no new rows on a second run. To see the
  incremental path work, edit a CSV, raise the `updated_timestamp` values, and upload it again.
- `obt_b.sql` writes the full `walmart.silver_t.*` table names instead of `ref()`. dbt therefore
  does not know that the silver technical layer comes first, and only the DAG enforces the order.
- Source freshness gives a warning but never an error. The sample data carries fixed timestamps,
  so an error threshold would stop every run.

## Credentials

No credential is written in any tracked file. Everything reads from the environment.

| Variable | Used by |
|---|---|
| `DATABRICKS_HOST` | The `ingest_cdc` task and dbt |
| `DATABRICKS_TOKEN` | The `ingest_cdc` task and dbt |
| `DATABRICKS_HTTP_PATH` | dbt |
| `DATABRICKS_JOB_ID` | The `ingest_cdc` task |
| `FERNET_KEY` | Airflow, to encrypt stored connections |
| `POSTGRES_CONN_STRING` | `01_source/load_data.py`, on your machine, JDBC mode only |

The first five live in `04_orchestration/.env`, which `.gitignore` excludes. Use
[.env.example](.env.example) as the template.

## Author

Built by **Muhammad Syakeer bin Abdul Rahman**.

- GitHub: [@SyakeerRahman](https://github.com/SyakeerRahman)
