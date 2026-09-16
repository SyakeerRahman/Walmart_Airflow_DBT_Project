# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A medallion-architecture lakehouse demo: Postgres (raw OLTP) -> Databricks CDC job -> Databricks Unity Catalog `walmart` -> dbt (bronze/silver/gold) -> Airflow 3 orchestration. See [README.md](README.md) for the full build guide. Ingestion into bronze happens in a Databricks job that is **not in this repo**; Airflow only triggers it by job id.

## Layout

Directories are numbered in pipeline order. Keep that convention when adding one.

| Path | Role |
| --- | --- |
| [01_source/](01_source/) | Seed CSVs, Postgres DDL, `load_data.py` loader. One-time setup of the source OLTP DB. |
| [02_ingestion/](02_ingestion/) | Empty. The Databricks CDC job belongs here once written. |
| [03_transform/](03_transform/) | The dbt project. Mounted into containers at `/opt/airflow/dbt` via a `../03_transform` relative volume in compose. |
| [04_orchestration/](04_orchestration/) | Docker Compose Airflow 3.2 stack (CeleryExecutor + Postgres + Redis). |
| [04_orchestration/dags/orchestrate.py](04_orchestration/dags/orchestrate.py) | The only DAG. Drives every dbt step as a Bash task. |
| [docs/images/](docs/images/) | `banner.png` (README header), `architecture.png` (hand-drawn design notes). |

## Commands

Run from `04_orchestration/`:

```powershell
docker compose build            # after editing requirements.txt or Dockerfile
docker compose up -d            # UI at http://localhost:8080, login airflow/airflow
docker compose run --rm airflow-cli dags list
docker compose down -v          # wipes the Airflow metadata DB too
```

dbt runs **inside** a container, from the project dir (that is where `profiles.yml` lives, so `--profiles-dir` is never passed):

```powershell
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt run --select silver_t"
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt test --select products_t"   # one model's tests
docker compose exec airflow-worker bash -lc "cd /opt/airflow/dbt && dbt build --select obt_b+"      # model + downstream
```

`docker compose` needs `FERNET_KEY` set (compose reads it with no default). Copy `.env.example` to `04_orchestration/.env`; the real `.env` is gitignored.

## Pipeline shape

The DAG is one strict linear chain, no parallelism:

`ingest_cdc` -> `clean_target` -> `source_freshness` -> `silver_technical` -> `silver_technical_tests` -> `silver_business` -> `silver_business_tests` -> `gold_ephermeral` -> `gold_dimensions` (`dbt snapshot`) -> `gold_facts`

Layer contract, which drives where new models go:

1. **source** ([models/source/sources.yml](03_transform/models/source/sources.yml)) points at `walmart.bronze.*`, populated by the external Databricks CDC job.
2. **silver_t** (technical): one model per source table, `materialized='incremental'` keyed on the table PK, watermarked on `updated_timestamp > max(updated_timestamp)`. Adds only `processed_at`. `SELECT *`, no reshaping.
3. **silver_b** (business): a single wide OBT, [obt_b.sql](03_transform/models/silver_b/obt_b.sql). Built by a Jinja loop over a `configs` list of `{table, columns, alias, join_condition}` dicts. To add a column or a joined table, edit that list, not the SELECT at the bottom.
4. **gold/ephemeral**: `materialized='ephemeral'`, one per entity, `SELECT DISTINCT` off `ref('obt_b')`. They exist purely as snapshot inputs and never land as tables.
5. **snapshots/**: YAML-only snapshots (dbt 1.9+ style) turning each ephemeral model into a SCD2 dim in `walmart.gold`, timestamp strategy on that entity's `*_updated_timestamp`, with `dbt_valid_to_current` set to `9999-12-31` instead of NULL.
6. **gold/fact**: [fact_orders.sql](03_transform/models/gold/fact/fact_orders.sql), keys plus measures off `ref('obt_b')`.

## Conventions worth keeping

- Schema routing is per-directory in [dbt_project.yml](03_transform/dbt_project.yml) (`silver_t`, `silver_b`, `gold`). [macros/custom_schema.sql](03_transform/macros/custom_schema.sql) overrides dbt's default so `+schema:` is used **verbatim**, with no `<target_schema>_` prefix. Any `+schema` you add is the literal Databricks schema name.
- `obt_b.sql` hardcodes fully-qualified `walmart.silver_t.*` table names instead of `ref()`, so dbt does not see silver_t -> silver_b as a dependency. The DAG's task order is what enforces it. Keep new cross-layer edges in the DAG, or switch the whole file to `ref()`.
- Singular tests live in [tests/](03_transform/tests/) and use `{{ config(severity='warn') }}` so a null key does not fail the run. Generic tests are in `models/silver_t/properties.yml`.
- `clean_target` deletes `target/` and `logs/` on every run, so dbt never reuses a stale manifest.
- **No credential is hardcoded anywhere.** `profiles.yml` uses `env_var()`; the DAG uses a bare `WorkspaceClient()` (the SDK reads `DATABRICKS_HOST`/`DATABRICKS_TOKEN` from the env) plus `os.environ` for `DATABRICKS_JOB_ID`; `load_data.py` reads `POSTGRES_CONN_STRING`. All container values come from `04_orchestration/.env` via the compose `env_file`. Keep it that way - never write a literal token into a tracked file.

## Known rough edges

- `gold_ephermeral` runs `dbt run --select gold/ephermeral`, but the directory is `gold/ephemeral`. The selector matches nothing; the task passes because the models are ephemeral anyway and get inlined by `gold_facts`.
- `dbt source freshness` runs but `sources.yml` declares no `loaded_at_field` or `freshness` block.
- `02_ingestion/` is empty. The Databricks CDC job that fills `walmart.bronze.*` is not in this repo yet.
- `04_orchestration/requirements.txt` is UTF-16 encoded. Rewrite it as UTF-8 if you touch it, or `pip install` in the Docker build will misread it.
