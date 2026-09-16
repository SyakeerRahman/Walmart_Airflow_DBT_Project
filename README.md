# Walmart Data Engineering Project

![Walmart Data Engineering with Airflow and dbt](docs/images/banner.png)

An end-to-end lakehouse pipeline. A Databricks job moves changed rows into a bronze Delta layer.
dbt builds the silver and gold layers. Apache Airflow runs the 10 steps in order.

The guide below takes you from an empty workspace to a verified gold layer. Every step shows the
output you must see before you continue.

## Architecture

```
PostgreSQL (raw schema)  or  CSV files in a Unity Catalog volume
        |  CDC job (PySpark, runs in Databricks)
        v
walmart.bronze.*            6 Delta tables, one for each source table
        |  dbt run --select silver_t
        v
walmart.silver_t.*          6 incremental tables. Grain: the source primary key
        |  dbt run --select silver_b
        v
walmart.silver_b.obt_b      1 wide table. Grain: one row for each order item
        |  dbt compile + dbt snapshot + dbt run
        v
walmart.gold.*              5 SCD2 dimensions + 1 fact table
```

Airflow triggers each step. The Airflow stack runs in Docker on your machine. Databricks does all
of the compute, so your machine needs only Docker.

**The grain matters.** `obt_b` holds one row for each order item, 30021 rows. A join that breaks
that rule multiplies every measure. Read [Design notes](#design-notes) before you add a table.

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
  models/source/     Bronze source declarations and freshness
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

scripts/
  setup_databricks.py  Builds the Databricks side in one command

docs/images/         Diagrams and the banner
```

## What you need first

| Item | Purpose |
|---|---|
| Docker Desktop, 4 GB of memory or more | It runs the Airflow stack. |
| A Databricks workspace with Unity Catalog | It runs the CDC job and the dbt models. |
| Python 3.9 or later | It runs the setup script. |
| Git | It clones this repository. |
| A PostgreSQL database | JDBC mode only. Skip it for volume mode. |

**Databricks Free Edition is enough.** Free Edition gives you serverless compute only. Serverless
cannot install a JDBC driver and cannot open a connection to an external database. Volume mode
exists for that reason, and it needs no PostgreSQL. Follow volume mode, and skip Step 1 and
Step 2.

## Two ways to build it

| Path | Time | Use it when |
|---|---|---|
| [Quick start](#quick-start) | About 30 minutes | You want a working pipeline now. |
| [Full build](#full-build) | About 90 minutes | You want to understand each file. |

Both paths end at the same place.

---

# Quick start

## A. Get your 3 Databricks values

**DATABRICKS_HOST**

Look at the browser address bar. Copy the address up to `.com` and drop the rest:

```
https://dbc-a1b2c3d4-e5f6.cloud.databricks.com
```

**DATABRICKS_TOKEN**

1. Click your avatar, top right.
2. **Settings**.
3. Left menu: **Developer**.
4. **Access tokens** -> **Manage** -> **Generate new token**.
5. Comment: `walmart-project`. Click **Generate**.
6. Copy the value now. Databricks shows it one time. It starts with `dapi`.

**DATABRICKS_HTTP_PATH**

1. Left sidebar: **SQL Warehouses**.
2. Click your warehouse. Free Edition names it **Serverless Starter Warehouse**.
3. **Connection details** tab.
4. Copy the **HTTP path** row. It looks like `/sql/1.0/warehouses/a1b2c3d4e5f6g7h8`.

## B. Write the environment file

Copy the template:

```powershell
copy .env.example 04_orchestration\.env
```

Open `04_orchestration/.env` and fill in the 3 values. Use no quotation marks and no spaces
around the `=` sign:

```
AIRFLOW_UID=50000
FERNET_KEY=
DATABRICKS_HOST=https://dbc-a1b2c3d4-e5f6.cloud.databricks.com
DATABRICKS_TOKEN=dapi_PASTE_YOUR_TOKEN_HERE
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/a1b2c3d4e5f6g7h8
DATABRICKS_JOB_ID=
```

Generate the Fernet key and paste it after `FERNET_KEY=`:

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

`FERNET_KEY` is an Airflow value, not a Databricks value. Airflow keeps connection passwords in
its own database and encrypts them with this key. Generate it one time and keep it.

`.gitignore` excludes `.env`. Never commit it.

## C. Build the Databricks side

```powershell
pip install databricks-sdk

$env:DATABRICKS_HOST  = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"
$env:DATABRICKS_TOKEN = "dapi_PASTE_YOUR_TOKEN_HERE"
$env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/a1b2c3d4e5f6g7h8"

python scripts\setup_databricks.py
```

[scripts/setup_databricks.py](scripts/setup_databricks.py) creates the catalog, the 4 schemas and
the volume, uploads the 6 CSV files and the notebook, and creates the job. The script is
idempotent, so you can run it again after a failure.

Expected output:

```
Workspace: https://dbc-a1b2c3d4-e5f6.cloud.databricks.com/
User:      you@example.com

[1/6] Catalog 'walmart'
  waiting for the SQL warehouse to start, this can take a minute
  ready

[2/6] Schemas in 'walmart'
  bronze: ready
  silver_t: ready
  silver_b: ready
  gold: ready

[3/6] Volume 'walmart.bronze.landing'
  ready at /Volumes/walmart/bronze/landing

[4/6] CSV files to /Volumes/walmart/bronze/landing
  customers.csv: uploaded, 256 KB
  stores.csv: uploaded, 2 KB
  products.csv: uploaded, 40 KB
  employees.csv: uploaded, 29 KB
  orders.csv: uploaded, 985 KB
  order_items.csv: uploaded, 2139 KB

[5/6] Notebook 'cdc_bronze'
  uploaded to /Users/you@example.com/cdc_bronze

[6/6] Job 'walmart_cdc_bronze'
  created

============================================================
Setup complete.

  DATABRICKS_JOB_ID=857856281992875

Put that line in 04_orchestration/.env.
Then run the job one time: Workflows -> walmart_cdc_bronze -> Run now
============================================================
```

Step 1 can take a minute, because the SQL warehouse starts from cold.

Put the job ID in `04_orchestration/.env`:

```
DATABRICKS_JOB_ID=857856281992875
```

## D. Start Airflow

```powershell
cd 04_orchestration
docker compose build
docker compose up -d
```

The first build takes 5 to 10 minutes. It downloads the Airflow image and installs dbt.

Wait one minute, then check every container:

```powershell
docker compose ps
```

All 7 must say `healthy`:

```
SERVICE                 STATUS
airflow-apiserver       Up 52 seconds (healthy)
airflow-dag-processor   Up 52 seconds (healthy)
airflow-scheduler       Up 52 seconds (healthy)
airflow-triggerer       Up 52 seconds (healthy)
airflow-worker          Up 30 seconds (healthy)
postgres                Up About a minute (healthy)
redis                   Up About a minute (healthy)
```

If a container says `Restarting`, go to [Troubleshooting](#troubleshooting).

## E. Test the dbt connection

```powershell
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt debug"
```

The last lines must be:

```
  Connection:
    host: dbc-a1b2c3d4-e5f6.cloud.databricks.com
    http_path: /sql/1.0/warehouses/a1b2c3d4e5f6g7h8
    catalog: walmart
    schema: dbt_schema
  Registered adapter: databricks=1.12.5
  Connection test: [OK connection ok]
  All checks passed!
```

The test takes about 6 seconds. Note that `host` shows no `https://` prefix. That is correct. See
[Design notes](#design-notes).

## F. Run the pipeline

1. Open http://localhost:8080.
2. Log in. The user name is `airflow` and the password is `airflow`.
3. Find the `orchestrate` DAG and set the toggle to on.
4. Click the play button to start a run.
5. Open the **Graph** view.

The 10 tasks run one after the other. A full run takes 5 to 6 minutes.

Go to [Verify the result](#verify-the-result).

---

# Full build

## Step 1 - Create the source database

**Skip this step in volume mode.**

1. Create an empty PostgreSQL database. Neon, Supabase and Aiven all give a free instance.
2. Create the schema:

   ```sql
   CREATE SCHEMA raw;
   SET search_path TO raw;
   ```

3. Run [01_source/ddl/walmart_schema.sql](01_source/ddl/walmart_schema.sql). Set `search_path`
   first, because the DDL file does not qualify the table names.

The DDL file creates 6 tables: `customers`, `stores`, `products`, `employees`, `orders` and
`order_items`. Every table has a `created_timestamp`, an `updated_timestamp` and an `is_active`
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

Expected output:

```
Loading customers.csv into raw.customers...
[ok] loaded customers.csv
Loading stores.csv into raw.stores...
[ok] loaded stores.csv
Loading products.csv into raw.products...
[ok] loaded products.csv
Loading employees.csv into raw.employees...
[ok] loaded employees.csv
Loading orders.csv into raw.orders...
[ok] loaded orders.csv
Loading order_items.csv into raw.order_items...
[ok] loaded order_items.csv

All data loaded.
```

Row counts: 2000 customers, 25 stores, 500 products, 250 employees, 10000 orders and 30021 order
items. The script stops with a clear message if the variable is empty.

## Step 3 - Prepare Databricks

1. Create a Unity Catalog catalog with the name `walmart`.
2. Create 4 schemas in it: `bronze`, `silver_t`, `silver_b` and `gold`.
3. Create a SQL warehouse. Copy its **Server hostname** and its **HTTP path**.
4. Create a personal access token. Copy the value now, because Databricks shows it one time.

The schema names are not free choices. [dbt_project.yml](03_transform/dbt_project.yml) and the
snapshot files write these exact names.

**On a workspace with Default Storage**, which includes Free Edition, use SQL and not the catalog
REST API:

```sql
CREATE CATALOG IF NOT EXISTS walmart;
CREATE SCHEMA  IF NOT EXISTS walmart.bronze;
CREATE SCHEMA  IF NOT EXISTS walmart.silver_t;
CREATE SCHEMA  IF NOT EXISTS walmart.silver_b;
CREATE SCHEMA  IF NOT EXISTS walmart.gold;
```

The REST API asks for a storage location on such a workspace and fails. SQL does not ask.

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

1. Create the volume:

   ```sql
   CREATE VOLUME IF NOT EXISTS walmart.bronze.landing;
   ```

2. Upload the 6 files from [01_source/data/](01_source/data/) to
   `/Volumes/walmart/bronze/landing/`. Use **Catalog -> walmart -> bronze -> landing -> Upload**.
   Keep the file names.
3. Confirm the upload:

   ```sql
   LIST '/Volumes/walmart/bronze/landing/';
   ```

4. Add a job parameter named `source_mode` with the value `volume`.

Volume mode declares the column types in the `SCHEMAS` dictionary, because type inference on a
CSV file is not reliable. Those types mirror
[walmart_schema.sql](01_source/ddl/walmart_schema.sql). Keep the two in agreement.

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
3. Run the job one time by hand.

Expected output on the first run:

```
Source mode: volume
customers: full load, 2000 rows
stores: full load, 25 rows
products: full load, 500 rows
employees: full load, 250 rows
orders: full load, 10000 rows
order_items: full load, 30021 rows

Done. 42796 rows written to walmart.bronze.
```

Expected output on every later run, while the source does not change:

```
Source mode: volume
customers: changes after 2026-09-15 22:14:07, 0 rows
stores: changes after 2026-09-11 08:02:55, 0 rows
products: changes after 2026-09-14 19:40:12, 0 rows
employees: changes after 2026-09-13 11:27:33, 0 rows
orders: changes after 2026-09-15 23:51:48, 0 rows
order_items: changes after 2026-09-15 23:58:02, 0 rows

Done. 0 rows written to walmart.bronze.
```

That second output is correct. It shows the watermark at work.

4. Copy the job ID from the job page URL. Step 9 puts it in `.env`.

Check the result:

```sql
SELECT COUNT(*) FROM walmart.bronze.orders;   -- 10000
```

## Step 5 - Create the dbt project

The finished dbt project is in [03_transform/](03_transform/). To build it from nothing, run
`dbt init walmart_project` and then add the files below.

### 5a - Connection profile

Put [profiles.yml](03_transform/profiles.yml) **inside** the project directory, not in `~/.dbt`.
dbt reads the working directory, so every command in this project works without the
`--profiles-dir` flag.

The file holds no credentials. The `env_var()` function reads them at run time:

```yaml
walmart_project:
  outputs:
    dev:
      type: databricks
      catalog: walmart
      schema: dbt_schema
      threads: 1
      host: "{{ env_var('DATABRICKS_HOST') | replace('https://', '') | replace('/', '') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
  target: dev
```

You set these 3 variables one time, in `04_orchestration/.env`, in Step 9.

`DATABRICKS_HOST` keeps the `https://` prefix, because the Databricks SDK in the DAG needs it.
This adapter adds its own prefix, so the filters remove the one in the variable. Without the
filters the adapter builds the address `https://https://...`, and it waits more than 2 minutes on
a failed probe before it connects.

### 5b - Schema routing

[dbt_project.yml](03_transform/dbt_project.yml) sends each model directory to its own schema:

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
macro [macros/custom_schema.sql](03_transform/macros/custom_schema.sql) overrides that behaviour.
It returns the custom name without a change, so the models land in `walmart.silver_t` and not in
`walmart.dbt_schema_silver_t`. Add this macro before you run any model.

### 5c - Sources

[models/source/sources.yml](03_transform/models/source/sources.yml) points dbt at the 6 bronze
tables from Step 4. It also declares freshness:

```yaml
    loaded_at_field: updated_timestamp
    freshness:
      warn_after: {count: 24, period: hour}
```

The CDC job carries `updated_timestamp` over from the source, so that column shows the age of the
newest row. There is no `error_after` threshold. The sample data has fixed timestamps, and an
error threshold would stop every run.

## Step 6 - Build the silver technical layer

Add one model for each source table in `models/silver_t/`. Each model follows the same pattern:

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
[models/silver_t/properties.yml](03_transform/models/silver_t/properties.yml). The file tests
`product_id` and `order_id` for `not_null` and `unique`.

Expected output from `dbt run --select silver_t` on the first run:

```
1 of 6 OK created sql incremental model silver_t.customers_t ..... [OK in 23.12s]
2 of 6 OK created sql incremental model silver_t.employees_t ..... [OK in 12.26s]
3 of 6 OK created sql incremental model silver_t.order_items_t ... [OK in 11.56s]
4 of 6 OK created sql incremental model silver_t.orders_t ........ [OK in 11.73s]
5 of 6 OK created sql incremental model silver_t.products_t ...... [OK in 11.39s]
6 of 6 OK created sql incremental model silver_t.stores_t ........ [OK in 11.68s]

Completed successfully

Done. PASS=6 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=6
```

The first model takes longer, because the SQL warehouse starts from cold.

## Step 7 - Build the One Big Table

[models/silver_b/obt_b.sql](03_transform/models/silver_b/obt_b.sql) joins 5 silver tables into 1
wide table. The model is metadata driven. A Jinja list holds one dictionary for each table:

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
- **Every join must keep the grain at one row for each order item.** Read
  [Design notes](#design-notes) before you add a table.

Put a Jinja comment outside the `{% set %}` block. The list is an expression, not template text,
so a `{# ... #}` comment inside it stops compilation with `unexpected char '#'`.

**Important:** this model writes the full table names, such as `walmart.silver_t.orders_t`. It
does not use `ref()`. For this reason, dbt does not know that the silver technical layer comes
first. The Airflow DAG controls the order instead.

## Step 8 - Build the gold layer

The gold layer has 3 parts.

### 8a - Ephemeral models

Add 5 models in `models/gold/ephemeral/`, one for each entity. Four of them do a `SELECT DISTINCT`
on `ref('obt_b')`:

```sql
SELECT DISTINCT
    customer_id,
    customer_first_name,
    CURRENT_TIMESTAMP() AS customer_gold_processed_at
FROM {{ ref('obt_b') }}
```

[eph_employees.sql](03_transform/models/gold/ephemeral/eph_employees.sql) is the exception. It
reads `ref('employees_t')`, because employees are not in the OBT. See
[Design notes](#design-notes).

These models are ephemeral. dbt does not write them to the warehouse. dbt puts their SQL into the
model that comes next. `dbt run` therefore builds nothing for them, and the DAG uses
`dbt compile` instead, which still finds a broken model.

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

Expected output from `dbt snapshot`:

```
1 of 5 OK snapshotted gold.dim_customers ..... [OK in 10.74s]
2 of 5 OK snapshotted gold.dim_employees ..... [OK in 8.82s]
3 of 5 OK snapshotted gold.dim_orders ........ [OK in 8.79s]
4 of 5 OK snapshotted gold.dim_products ...... [OK in 9.09s]
5 of 5 OK snapshotted gold.dim_stores ........ [OK in 9.15s]

Done. PASS=5 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=5
```

### 8c - Fact table

[fact_orders.sql](03_transform/models/gold/fact/fact_orders.sql) holds 5 keys and 4 measures from
`ref('obt_b')`. It has no `employee_id`, because an order has no employee in this schema.

Add the singular test [tests/test_obt.sql](03_transform/tests/test_obt.sql). The test finds a row
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

   The last line is the important one. The dbt project lives one directory up, and the containers
   must see it at `/opt/airflow/dbt`.

3. Declare the 4 Databricks variables in the `environment:` block, so the contract is visible:

   ```yaml
       DATABRICKS_HOST: ${DATABRICKS_HOST:-}
       DATABRICKS_TOKEN: ${DATABRICKS_TOKEN:-}
       DATABRICKS_HTTP_PATH: ${DATABRICKS_HTTP_PATH:-}
       DATABRICKS_JOB_ID: ${DATABRICKS_JOB_ID:-}
   ```

4. Write [requirements.txt](04_orchestration/requirements.txt):

   ```
   dbt-core>=1.11.11
   dbt-databricks>=1.12.1
   ```

   Save this file as UTF-8. `pip` can fail to read a UTF-16 requirements file during the build.

   **Do not add `apache-airflow` here.** The base image already has it. `pip` upgrades Airflow but
   not the providers that come with the image, and the Celery worker then stops at start.
   `BashOperator` comes with Airflow, so no operator package is necessary.

5. Write [Dockerfile](04_orchestration/Dockerfile):

   ```dockerfile
   FROM apache/airflow:3.2.2
   USER root
   RUN apt-get update && apt-get install -y gcc && apt-get clean
   USER airflow
   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt
   ```

   The Databricks SDK arrives with `dbt-databricks`, so the DAG can import it.

6. Copy `.env.example` to `.env` and fill in all 6 values. See
   [Quick start section B](#b-write-the-environment-file).

7. Build the image and start the stack:

   ```powershell
   docker compose build
   docker compose up -d
   ```

8. Open http://localhost:8080. The user name is `airflow` and the password is `airflow`.

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

- `ingest_cdc` is a Python task. It calls `WorkspaceClient.jobs.run_now()` and then polls
  `jobs.get_run()` every 5 seconds. It stops the DAG when the job result is not `SUCCESS`.
- Every dbt step is a `BashOperator` with `cwd='/opt/airflow/dbt'`. The `cwd` parameter puts dbt
  in the project directory, so dbt finds `profiles.yml` there.

The DAG holds no credentials. `WorkspaceClient()` takes `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
from the container environment, and the task reads `DATABRICKS_JOB_ID` the same way. The task
fails with a clear message if the job ID is missing.

`clean_target` removes the `target` and `logs` directories before each run. This step stops dbt
from the use of an old manifest.

**Write every selector in dot form**, such as `gold.fact`. A value with a slash is a path
selector, and dbt measures that path from the project root. A value such as `gold/fact` matches no
model, dbt prints `Nothing to do`, and the task still reports success. See
[Design notes](#design-notes).

## Step 11 - Run the pipeline

1. Open the Airflow user interface.
2. Turn on the `orchestrate` DAG.
3. Start a manual run.
4. Open the **Graph** view.

A full run takes 5 to 6 minutes.

Check the task states from the command line:

```powershell
docker compose exec airflow-scheduler airflow tasks states-for-dag-run orchestrate <run_id>
```

All 10 must say `success`:

```
task_id                  | state
=========================+=========
ingest_cdc               | success
clean_target             | success
source_freshness         | success
silver_technical         | success
silver_technical_tests   | success
silver_business          | success
silver_business_tests    | success
gold_ephemeral           | success
gold_dimensions          | success
gold_facts               | success
```

**A green task is not proof of correct data.** Three separate faults in this project passed as
green. Always run the queries below.

---

# Verify the result

Run this in a Databricks SQL editor:

```sql
SELECT 'bronze.orders'      AS tbl, COUNT(*) AS n FROM walmart.bronze.orders
UNION ALL SELECT 'silver_t.orders_t',      COUNT(*) FROM walmart.silver_t.orders_t
UNION ALL SELECT 'silver_t.order_items_t', COUNT(*) FROM walmart.silver_t.order_items_t
UNION ALL SELECT 'silver_b.obt_b',         COUNT(*) FROM walmart.silver_b.obt_b
UNION ALL SELECT 'gold.fact_orders',       COUNT(*) FROM walmart.gold.fact_orders
UNION ALL SELECT 'gold.dim_customers',     COUNT(*) FROM walmart.gold.dim_customers
UNION ALL SELECT 'gold.dim_employees',     COUNT(*) FROM walmart.gold.dim_employees
UNION ALL SELECT 'gold.dim_products',      COUNT(*) FROM walmart.gold.dim_products
UNION ALL SELECT 'gold.dim_stores',        COUNT(*) FROM walmart.gold.dim_stores
UNION ALL SELECT 'gold.dim_orders',        COUNT(*) FROM walmart.gold.dim_orders
ORDER BY tbl;
```

Expected result:

| Table | Rows |
|---|---|
| `bronze.orders` | 10000 |
| `silver_t.orders_t` | 10000 |
| `silver_t.order_items_t` | 30021 |
| `silver_b.obt_b` | 30021 |
| `gold.fact_orders` | 30021 |
| `gold.dim_customers` | 1991 |
| `gold.dim_employees` | 250 |
| `gold.dim_products` | 500 |
| `gold.dim_stores` | 25 |
| `gold.dim_orders` | 30021 |

`dim_customers` holds 1991 rows and not 2000. That number is correct. Only 1991 customers have an
order, and the OBT starts from orders.

## Three integrity checks

These 3 queries find the faults that a row count hides.

**1. Revenue must reconcile.** A fan-out in the OBT shows here first:

```sql
SELECT
  (SELECT ROUND(SUM(total_amount)) FROM (
      SELECT DISTINCT order_id, total_amount FROM walmart.gold.fact_orders)) AS through_fact,
  (SELECT ROUND(SUM(total_amount)) FROM walmart.silver_t.orders_t)           AS truth;
```

Expected: both columns show `18947797`.

**2. No duplicated order items.** The result must be 0 rows:

```sql
SELECT order_item_id, COUNT(*) AS n
FROM walmart.gold.fact_orders
GROUP BY order_item_id HAVING COUNT(*) > 1;
```

**3. SCD2 columns must exist and carry the end date.**

```sql
SELECT customer_id, dbt_valid_from, dbt_valid_to
FROM walmart.gold.dim_customers LIMIT 2;
```

Expected:

```
customer_id | dbt_valid_from           | dbt_valid_to
------------+--------------------------+--------------------------
          1 | 2026-06-21T10:15:55.000Z | 9999-12-31T00:00:00.000Z
          2 | 2026-06-26T18:24:36.000Z | 9999-12-31T00:00:00.000Z
```

## See the incremental path work

Volume mode reads a static CSV file, so a second run finds no new rows. To watch the merge work:

1. Open `01_source/data/orders.csv`.
2. Raise the `updated_timestamp` value on 5 rows to today.
3. Upload the file again to `/Volumes/walmart/bronze/landing/`.
4. Start the DAG again.

The CDC job then reports `changes after ..., 5 rows`, and only those 5 rows move through the
pipeline. `dim_orders` gets a second version of each changed row, and the old version gets a real
`dbt_valid_to` date.

---

# Run one step by hand

The dbt commands run inside the containers:

```powershell
cd 04_orchestration

# One layer
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt run --select silver_t"

# One model
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt run --select orders_t"

# One model and everything after it
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt build --select obt_b+"

# The tests of one model
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt test --select products_t"

# The dimensions
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt snapshot"
```

Stop the stack when you finish:

```powershell
docker compose down
```

Add `-v` to remove the Airflow database as well. You must add `-v` after an Airflow version
change.

---

# Troubleshooting

## The Celery worker restarts without stop

```
AirflowOptionalProviderFeatureException: Failed to import
providers_configuration_loaded. This feature is only available in Airflow
versions >= 2.8.0
```

**Cause:** `requirements.txt` holds `apache-airflow`. `pip` upgraded the core package, but the
providers stayed at the versions that the image pins.

**Fix:** remove `apache-airflow` from `requirements.txt`. Set the base image to the version you
want. Then run `docker compose build` and `docker compose down -v`.

## Every service refuses to start after a version change

```
Database migration required. Please run `airflow db migrate`. Make sure the
command is run using Airflow version 3.2.2.
```

**Cause:** the metadata database keeps the schema of the version that ran before.

**Fix:**

```powershell
docker compose down -v
docker compose up -d
```

`-v` removes the database volume. The volume holds run history only.

## The catalog does not get created

```
InvalidState: Metastore storage root URL does not exist. Default Storage is
enabled in your account.
```

**Cause:** the catalog REST API asks for a storage location on a Default Storage workspace.

**Fix:** use SQL instead: `CREATE CATALOG IF NOT EXISTS walmart;`

## A dbt task reports success but builds nothing

```
[WARNING]: The selection criterion 'gold/fact' does not match any enabled nodes
[WARNING]: Nothing to do. Try checking your model configs and model specification args
```

**Cause:** a selector with a slash is a path selector, and dbt measures the path from the project
root. dbt returns 0 for an empty selection, so Airflow marks the task green.

**Fix:** write the selector in dot form, such as `gold.fact`. Read the task log after a change to
a selector.

## dbt waits about 2 minutes and then connects

```
SPOG discovery probe to 'https://https://dbc-....cloud.databricks.com//.well-known/...' failed
```

**Cause:** `DATABRICKS_HOST` holds the `https://` prefix, and the adapter adds a second one.

**Fix:** remove the prefix in `profiles.yml` with the `replace` filters. See
[Step 5a](#5a---connection-profile).

## The OBT model does not compile

```
Compilation Error in model obt_b
  unexpected char '#' at 2851
```

**Cause:** a `{# ... #}` comment sits inside the `{% set configs = [...] %}` block. That block is
an expression, not template text.

**Fix:** move the comment above the `{% set %}` line.

## dbt cannot connect

Check the 3 variables inside the container:

```powershell
docker compose exec airflow-worker bash -lc "env | grep DATABRICKS_ | cut -c1-40"
```

If they are empty, `.env` is not in `04_orchestration/`, or a line has spaces around the `=` sign.

If they are present, start the SQL warehouse by hand. A warehouse that sleeps takes about 1 minute
to answer.

---

# Design notes

## The grain of the OBT

`obt_b` holds one row for each order item, 30021 rows. Every `LEFT JOIN` in the `configs` list
must be one to one against that grain. Confirm the relationship before you add a table.

An early version of this project joined employees on `o.store_id = e.store_id`. Each store has 10
employees, so every order row matched 10 of them:

| Measure | With the employee join | Correct |
|---|---|---|
| Rows in `obt_b` | 300513 | 30021 |
| `SUM(total_amount)` | 694628977 | 18947797 |

Revenue came out 36 times too large. The schema gives no relationship between an order and an
employee: `orders` carries `customer_id` and `store_id` only. No join condition is correct, so
employees are out of the OBT. `eph_employees` reads `employees_t` instead, and `fact_orders`
carries no `employee_id`.

## A green task is not proof

Three faults in this project passed as green:

1. `dbt run --select gold/ephermeral`, a spelling mistake, matched no model.
2. `dbt run --select gold/fact`, a path selector, matched no model. `fact_orders` did not exist,
   and the run was still green.
3. The employee fan-out above made every model build without an error.

dbt returns 0 when a selection is empty, and Airflow cannot see the difference between work and no
work. Run the queries in [Verify the result](#verify-the-result) after each change.

## Why obt_b avoids ref()

`obt_b.sql` writes the full `walmart.silver_t.*` names. dbt therefore does not know that the
silver technical layer comes first, and only the DAG holds that order. Keep new cross layer edges
in the DAG, or change the whole file to `ref()`.

## Why the ephemeral task uses compile

An ephemeral model never lands as a table. `dbt run` has nothing to build for it and reports
`Nothing to do`. `dbt compile` renders the SQL, so a broken ephemeral model stops the pipeline at
`gold_ephemeral` and not inside `dbt snapshot` two steps later.

---

# Credentials

No credential is written in any tracked file. Everything reads from the environment.

| Variable | Used by |
|---|---|
| `DATABRICKS_HOST` | The `ingest_cdc` task and dbt |
| `DATABRICKS_TOKEN` | The `ingest_cdc` task and dbt |
| `DATABRICKS_HTTP_PATH` | dbt |
| `DATABRICKS_JOB_ID` | The `ingest_cdc` task |
| `FERNET_KEY` | Airflow, to encrypt stored connections |
| `POSTGRES_CONN_STRING` | `01_source/load_data.py`, on your machine, JDBC mode only |

The first 5 live in `04_orchestration/.env`, which `.gitignore` excludes. Use
[.env.example](.env.example) as the template.

`cdc_bronze.py` in JDBC mode reads 3 more values from the Databricks secret scope `walmart`.

# Known limitations

- The `02_ingestion` job runs outside the Airflow stack. Airflow starts it and polls, so a failure
  inside Databricks shows in the Databricks run page and not in the Airflow log.
- JDBC mode needs a classic cluster that can install a Maven library and reach your PostgreSQL
  host. Serverless compute allows neither. Volume mode exists for that reason.
- Volume mode reads a static CSV file, so it shows no new rows on a second run.
- Source freshness gives a warning but never an error. The sample data carries fixed timestamps,
  so an error threshold would stop every run.
- `obt_b.sql` writes the full table names instead of `ref()`, so only the DAG holds the order of
  the layers.
- `fact_orders` has no `employee_id`. The schema gives no relationship between an order and an
  employee, so that key does not belong at this grain.

# Author

Built by **Muhammad Syakeer bin Abdul Rahman**.

- GitHub: [@SyakeerRahman](https://github.com/SyakeerRahman)
