# Walmart Data Engineering Project

![Walmart Data Engineering with Airflow and dbt](walmart_dbt.png)

An end-to-end lakehouse pipeline. PostgreSQL holds the operational data. A Databricks job moves
changed rows into a bronze Delta layer. dbt builds the silver and gold layers. Apache Airflow
runs the steps in order.

The guide below takes you from an empty database to a verified gold layer in 11 steps.

## Architecture

```
PostgreSQL (raw schema)
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

## What you need first

| Item | Purpose |
|---|---|
| Docker Desktop, 4 GB of memory or more | It runs the Airflow stack. |
| A Databricks workspace with Unity Catalog | It runs the CDC job and the dbt models. |
| A PostgreSQL database that Databricks can reach | It is the operational source. |
| Python 3.9 or later, with `psycopg2` | It loads the sample data into PostgreSQL. |
| Git | It clones this repository. |

A free Databricks trial and a free hosted PostgreSQL instance are sufficient.

---

## Step 1 - Create the source database

1. Create an empty PostgreSQL database.
2. Create the schema:

   ```sql
   CREATE SCHEMA raw;
   ```

3. Run [walmart_dataset/ddl/walmart_schema.sql](walmart_dataset/ddl/walmart_schema.sql) in that
   schema. Set `search_path` to `raw` first, because the DDL file does not qualify the table
   names.

The DDL file creates 6 tables: `customers`, `stores`, `products`, `employees`, `orders`, and
`order_items`. Every table has a `created_timestamp`, an `updated_timestamp`, and an `is_active`
column. The CDC job and the dbt incremental models both use `updated_timestamp`.

## Step 2 - Load the sample data

1. Open [walmart_dataset/load_data.py](walmart_dataset/load_data.py).
2. Replace `your_connection_string_here` with your PostgreSQL connection string.
3. Run the loader from the `walmart_dataset` directory:

   ```powershell
   cd walmart_dataset
   pip install psycopg2-binary
   python load_data.py
   ```

The script loads 6 CSV files with the `COPY` command. Expect 2000 customers, 25 stores, 500
products, 250 employees, 10000 orders, and 30021 order items.

## Step 3 - Prepare Databricks

1. Create a Unity Catalog catalog with the name `walmart`.
2. Create 4 schemas in it: `bronze`, `silver_t`, `silver_b`, and `gold`.
3. Create a SQL warehouse. Copy its **Server hostname** and its **HTTP path**.
4. Create a personal access token. Copy the token value now, because Databricks shows it one time.

The schema names are not free choices. [dbt_project.yml](airflow_dbt_project/walmart_project/dbt_project.yml)
and the snapshot files write these exact names.

## Step 4 - Build the CDC ingestion job

This repository does not contain the ingestion job. You create it in Databricks. The Airflow DAG
only starts it by job ID and then waits for the result.

Write a PySpark notebook that does these actions for each of the 6 source tables:

1. Read the PostgreSQL table through the JDBC connector.
2. Select only the rows with an `updated_timestamp` that is later than the maximum
   `updated_timestamp` in the matching bronze table.
3. Merge those rows into `walmart.bronze.<table>` on the primary key. Use `MERGE INTO`, so the
   job updates the old rows and inserts the new rows.
4. On the first run, write the full table, because the bronze table does not exist yet.

Then do this:

1. Save the notebook as a Databricks job.
2. Run the job one time by hand, to fill the bronze layer.
3. Copy the job ID from the job page URL.

## Step 5 - Create the dbt project

The finished dbt project is in
[airflow_dbt_project/walmart_project/](airflow_dbt_project/walmart_project/). To build it from
nothing, run `dbt init walmart_project` and then add the files below.

### 5a - Connection profile

Put [profiles.yml](airflow_dbt_project/walmart_project/profiles.yml) **inside** the project
directory, not in `~/.dbt`. dbt reads the working directory, so every command in this project
works without the `--profiles-dir` flag.

```yaml
walmart_project:
  outputs:
    dev:
      type: databricks
      catalog: walmart
      host: your_databricks_host
      http_path: your_databricks_http_path
      token: your_databricks_token
      schema: dbt_schema
      threads: 1
  target: dev
```

Replace the 3 placeholder values with the values from Step 3.

### 5b - Schema routing

[dbt_project.yml](airflow_dbt_project/walmart_project/dbt_project.yml) sends each model directory
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
macro [macros/custom_schema.sql](airflow_dbt_project/walmart_project/macros/custom_schema.sql)
overrides that behavior. It returns the custom name without a change, so the models land in
`walmart.silver_t` and not in `walmart.dbt_schema_silver_t`. Add this macro before you run any
model.

### 5c - Sources

[models/source/sources.yml](airflow_dbt_project/walmart_project/models/source/sources.yml) points
dbt at the 6 bronze tables from Step 4.

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
[models/silver_t/properties.yml](airflow_dbt_project/walmart_project/models/silver_t/properties.yml).
The file tests `product_id` and `order_id` for `not_null` and `unique`.

## Step 7 - Build the One Big Table

[models/silver_b/obt_b.sql](airflow_dbt_project/walmart_project/models/silver_b/obt_b.sql) joins
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

[models/gold/fact/fact_orders.sql](airflow_dbt_project/walmart_project/models/gold/fact/fact_orders.sql)
holds the 6 keys and the 4 measures from `ref('obt_b')`.

Add the singular test
[tests/test_obt.sql](airflow_dbt_project/walmart_project/tests/test_obt.sql). The test finds a row
with a `NULL` key. Its severity is `warn`, so a problem does not stop the pipeline.

## Step 9 - Set up Airflow in Docker

Work in the `airflow_dbt_project` directory.

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
       - ${AIRFLOW_PROJ_DIR:-.}/walmart_project:/opt/airflow/walmart_project
   ```

   The last line is the important one. dbt must see the project at
   `/opt/airflow/walmart_project` inside the containers.

3. Write [requirements.txt](airflow_dbt_project/requirements.txt):

   ```
   airflow-operators>=0.11.0
   apache-airflow>=3.2.2
   dbt-core>=1.11.11
   dbt-databricks>=1.12.1
   ```

   Save this file as UTF-8. The current file is UTF-16, and `pip` can fail to read that encoding.

4. Write [Dockerfile](airflow_dbt_project/Dockerfile). It extends the Airflow image and installs
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

5. Write `.env`:

   ```
   AIRFLOW_UID=50000
   FERNET_KEY=<your key>
   ```

   The Compose file reads `FERNET_KEY` and gives it no default value. Generate a key with this
   command:

   ```powershell
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

6. Build the image and start the stack:

   ```powershell
   docker compose build
   docker compose up -d
   ```

7. Open http://localhost:8080. The user name is `airflow` and the password is `airflow`.

## Step 10 - Write the DAG

[dags/orchestrate.py](airflow_dbt_project/dags/orchestrate.py) holds 1 DAG with 10 tasks in a
straight line:

```
ingest_cdc -> clean_target -> source_freshness
           -> silver_technical -> silver_technical_tests
           -> silver_business  -> silver_business_tests
           -> gold_ephermeral  -> gold_dimensions -> gold_facts
```

Two task types do the work:

- `ingest_cdc` is a Python task. It calls `WorkspaceClient.jobs.run_now()` with your Databricks
  job ID. Then it polls `jobs.get_run()` every 5 seconds. It stops the DAG when the job result is
  not `SUCCESS`.
- Every dbt step is a `BashOperator` with `cwd='/opt/airflow/walmart_project'`. The `cwd`
  parameter puts dbt in the project directory, so dbt finds `profiles.yml` there.

Put your Databricks host, token, and job ID at the top of the `ingest_cdc` function.

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
docker compose exec airflow-worker bash -lc "cd /opt/airflow/walmart_project && dbt run --select silver_t"
docker compose exec airflow-worker bash -lc "cd /opt/airflow/walmart_project && dbt test --select products_t"
docker compose exec airflow-worker bash -lc "cd /opt/airflow/walmart_project && dbt snapshot"
```

## Known limitations

- The `gold_ephermeral` task runs `dbt run --select gold/ephermeral`. The directory name is
  `gold/ephemeral`. The selector matches no model. The task still reports success, because the
  models are ephemeral and `gold_facts` compiles them anyway.
- The `source_freshness` task runs `dbt source freshness`, but `sources.yml` declares no
  `loaded_at_field` and no `freshness` block.
- The credentials are placeholder strings in 3 files: `dags/orchestrate.py`, `profiles.yml`, and
  `load_data.py`. Move them to environment variables or to an Airflow connection before you push
  the code to a public repository.
- The `airflow_dbt_project/logs` directory is in Git, and the repository has no root
  `.gitignore`.

## Author

Built by **Muhammad Syakeer bin Abdul Rahman**.

- GitHub: [@SyakeerRahman](https://github.com/SyakeerRahman)
