r"""Create everything this project needs inside a Databricks workspace.

The script is idempotent. A second run changes nothing and reports the same
result, so it is safe to run again after a failure.

What it creates
---------------
1. The catalog `walmart`.
2. The schemas `bronze`, `silver_t`, `silver_b` and `gold`.
3. The volume `walmart.bronze.landing`.
4. The 6 CSV files, uploaded to that volume.
5. The notebook `cdc_bronze` in your workspace home directory.
6. A job named `walmart_cdc_bronze` that runs the notebook in volume mode.

It prints the job ID at the end. Put that value in DATABRICKS_JOB_ID in
04_orchestration/.env.

How to run it
-------------
    pip install databricks-sdk

    $env:DATABRICKS_HOST  = "https://dbc-xxxx.cloud.databricks.com"
    $env:DATABRICKS_TOKEN = "dapi..."
    python scripts\setup_databricks.py

The script never writes a credential to disk.
"""

import os
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.workspace import ImportFormat, Language

CATALOG = "walmart"
SCHEMAS = ["bronze", "silver_t", "silver_b", "gold"]
VOLUME_SCHEMA = "bronze"
VOLUME_NAME = "landing"
JOB_NAME = "walmart_cdc_bronze"
NOTEBOOK_NAME = "cdc_bronze"

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "01_source" / "data"
JOB_SOURCE = REPO_ROOT / "02_ingestion" / "cdc_bronze.py"

CSV_FILES = [
    "customers.csv",
    "stores.csv",
    "products.csv",
    "employees.csv",
    "orders.csv",
    "order_items.csv",
]


def step(number: int, message: str) -> None:
    print(f"\n[{number}/6] {message}")


def warehouse_id() -> str:
    """Take the warehouse ID from the end of the HTTP path."""
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    if not http_path:
        raise ValueError("DATABRICKS_HTTP_PATH is not set.")
    return http_path.rstrip("/").split("/")[-1]


def run_sql(w: WorkspaceClient, statement: str, timeout_seconds: int = 300) -> None:
    """Run one SQL statement and wait for the result.

    Unity Catalog DDL goes through SQL, not through the catalog API. On a
    workspace with Default Storage the API asks for a storage location, but SQL
    does not, which is the behaviour the web interface uses.
    """
    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id(), statement=statement, wait_timeout="30s"
    )

    deadline = time.time() + timeout_seconds
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            raise TimeoutError(f"Statement did not finish in {timeout_seconds}s: {statement}")
        time.sleep(3)
        response = w.statement_execution.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        message = response.status.error.message if response.status.error else "unknown error"
        raise RuntimeError(f"{statement}\n  -> {message}")


def create_catalog(w: WorkspaceClient) -> None:
    step(1, f"Catalog '{CATALOG}'")
    print("  waiting for the SQL warehouse to start, this can take a minute")
    run_sql(w, f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    print("  ready")


def create_schemas(w: WorkspaceClient) -> None:
    step(2, f"Schemas in '{CATALOG}'")
    for schema in SCHEMAS:
        run_sql(w, f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
        print(f"  {schema}: ready")


def create_volume(w: WorkspaceClient) -> str:
    step(3, f"Volume '{CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME}'")
    path = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}"
    run_sql(w, f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME}")
    print(f"  ready at {path}")
    return path


def upload_csv_files(w: WorkspaceClient, volume_path: str) -> None:
    step(4, f"CSV files to {volume_path}")
    for name in CSV_FILES:
        local = DATA_DIR / name
        if not local.exists():
            raise FileNotFoundError(f"Missing sample file: {local}")

        with open(local, "rb") as handle:
            w.files.upload(f"{volume_path}/{name}", handle, overwrite=True)
        size_kb = local.stat().st_size / 1024
        print(f"  {name}: uploaded, {size_kb:.0f} KB")


def upload_notebook(w: WorkspaceClient) -> str:
    step(5, f"Notebook '{NOTEBOOK_NAME}'")
    user = w.current_user.me().user_name
    notebook_path = f"/Users/{user}/{NOTEBOOK_NAME}"

    source = JOB_SOURCE.read_text(encoding="utf-8")
    w.workspace.upload(
        path=notebook_path,
        content=source.encode("utf-8"),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    print(f"  uploaded to {notebook_path}")
    return notebook_path


def create_job(w: WorkspaceClient, notebook_path: str) -> int:
    step(6, f"Job '{JOB_NAME}'")

    task = jobs.Task(
        task_key="cdc_bronze",
        notebook_task=jobs.NotebookTask(
            notebook_path=notebook_path,
            base_parameters={"source_mode": "volume"},
        ),
    )

    for existing in w.jobs.list(name=JOB_NAME):
        w.jobs.reset(job_id=existing.job_id, new_settings=jobs.JobSettings(
            name=JOB_NAME, tasks=[task]
        ))
        print(f"  updated existing job")
        return existing.job_id

    created = w.jobs.create(name=JOB_NAME, tasks=[task])
    print(f"  created")
    return created.job_id


def main() -> None:
    if not os.environ.get("DATABRICKS_HOST") or not os.environ.get("DATABRICKS_TOKEN"):
        sys.exit(
            "DATABRICKS_HOST and DATABRICKS_TOKEN must be set.\n"
            "See the docstring at the top of this file."
        )

    w = WorkspaceClient()
    print(f"Workspace: {os.environ['DATABRICKS_HOST']}")
    print(f"User:      {w.current_user.me().user_name}")

    create_catalog(w)
    create_schemas(w)
    volume_path = create_volume(w)
    upload_csv_files(w, volume_path)
    notebook_path = upload_notebook(w)
    job_id = create_job(w, notebook_path)

    print("\n" + "=" * 60)
    print("Setup complete.")
    print(f"\n  DATABRICKS_JOB_ID={job_id}")
    print("\nPut that line in 04_orchestration/.env.")
    print(f"Then run the job one time: Workflows -> {JOB_NAME} -> Run now")
    print("=" * 60)


if __name__ == "__main__":
    main()
