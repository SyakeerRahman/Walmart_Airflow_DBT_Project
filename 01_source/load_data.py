"""Load the sample CSV files into the raw schema of the source PostgreSQL database.

The connection string comes from the POSTGRES_CONN_STRING environment variable.
Set it before you run this script:

    PowerShell:  $env:POSTGRES_CONN_STRING = "postgresql://user:pass@host:5432/db"
    Git Bash:    export POSTGRES_CONN_STRING="postgresql://user:pass@host:5432/db"
"""

import os
import sys

import psycopg2

# CSV file to target table.
CSV_FILES = {
    "customers.csv": "raw.customers",
    "stores.csv": "raw.stores",
    "products.csv": "raw.products",
    "employees.csv": "raw.employees",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items",
}

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def main():
    conn_string = os.environ.get("POSTGRES_CONN_STRING")
    if not conn_string:
        sys.exit(
            "POSTGRES_CONN_STRING is not set.\n"
            "See the docstring at the top of this file for the export command."
        )

    conn = None
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        for csv_file, table_name in CSV_FILES.items():
            csv_path = os.path.join(DATA_DIR, csv_file)

            if not os.path.exists(csv_path):
                print(f"[skip] file not found: {csv_path}")
                continue

            print(f"Loading {csv_file} into {table_name}...")
            with open(csv_path, "r", encoding="utf-8") as handle:
                cursor.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    handle,
                )
            conn.commit()
            print(f"[ok] loaded {csv_file}")

        cursor.close()
        print("\nAll data loaded.")

    except Exception as error:
        print(f"Error: {error}")
        if conn is not None:
            conn.rollback()
        sys.exit(1)

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
