import argparse
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import os

## to be used in docker only

current_folder = os.path.dirname(os.path.abspath(__file__))

SOURCE_COLUMNS = {
    "initial_licenses": ["id", "customer_id", "type", "creation_date", "price", "renewable"],
    "license_changes": ["id", "license_id", "date", "price", "type", "renewable"],
}


def create_source_tables(engine):
    """Create (or upgrade) the source tables from `sql/create.sql`."""
    with open(os.path.join(current_folder, "sql", "create.sql"), "r") as f:
        sql_script = f.read()
    # run through the raw psycopg connection: the script holds several statements and a DO block
    raw = engine.raw_connection()
    try:
        raw.driver_connection.execute(sql_script)
        raw.commit()
    finally:
        raw.close()


def load_source(engine, df: pd.DataFrame, table: str, full_refresh: bool = False) -> int:
    """
    Upsert rows into a source table, stamping `ingested_at` on arrival.

    New ids are inserted, and rows whose content changed are updated; both get
    `ingested_at = now()`. Unchanged rows keep their original `ingested_at`, so
    re-loading the same file does not make the incremental pipeline reprocess
    them. With `full_refresh`, the table is emptied first (every row is new).

    Returns the number of rows inserted or updated.
    """
    columns = SOURCE_COLUMNS[table]
    batch = df[columns]
    rows = batch.astype(object).where(batch.notna(), None).to_dict(orient="records")

    data_cols = [c for c in columns if c != "id"]
    upsert_query = text(f"""
        INSERT INTO {table} AS t ({", ".join(columns)}, ingested_at)
        VALUES ({", ".join(f":{c}" for c in columns)}, now())
        ON CONFLICT (id) DO UPDATE SET
            {", ".join(f"{c} = EXCLUDED.{c}" for c in data_cols)},
            ingested_at = now()
        WHERE ({", ".join(f"t.{c}" for c in data_cols)})
              IS DISTINCT FROM ({", ".join(f"EXCLUDED.{c}" for c in data_cols)})
    """)

    with engine.begin() as connection:
        if full_refresh:
            connection.execute(text(f"TRUNCATE {table}"))
        if not rows:
            return 0
        result = connection.execute(upsert_query, rows)
    return result.rowcount


def read_sources(initial_licenses_path, license_changes_path):
    init_licenses = pd.read_csv(initial_licenses_path, header=0, index_col=False)
    license_changed = pd.read_csv(license_changes_path, header=0, index_col=False)
    init_licenses["creation_date"] = pd.to_datetime(init_licenses["creation_date"]).dt.date
    license_changed["date"] = pd.to_datetime(license_changed["date"]).dt.date
    return init_licenses, license_changed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Load CSV data into an PostgreSQL database."
        )

    parser.add_argument(
        "--initial-licenses",
        required=False,
        default = Path("/app/app/csv/initial_licenses.csv"),
        type=Path,
        help="Path to initial_licenses.csv",
    )

    parser.add_argument(
        "--license-changes",
        required=False,
        default=Path("/app/app/csv/license_changes.csv"),
        type=Path,
        help="Path to license_changes.csv",
    )

    parser.add_argument(
        "--db_host",
        default="localhost",
        type=Path,
        help="db host name (default: localhost), switch to `db` if using PostrgeSQL within a docker service",
    )

    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="empty the source tables before loading (every row gets a new ingested_at)",
    )

    parser.add_argument(
        "--checks",
        default=False,
        type=bool,
        help="check if data are stored accordingly",
    )
    args = parser.parse_args()

    load_dotenv(os.path.join(current_folder, "config", ".env.postgre"))

    # Load CSV
    init_licenses, license_changed = read_sources(args.initial_licenses, args.license_changes)

    db_host = str(args.db_host)
    if db_host == "localhost": # default
        db_host= os.getenv('DB_HOST', )
    # PostgreSQL connection
    engine = create_engine(
        f"postgresql+psycopg://"
         f"{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{db_host}:"
        f"{os.getenv('DB_PORT', '5432')}/"
        f"{os.getenv('DB_NAME')}"
    )

    create_source_tables(engine)
    n_init = load_source(engine, init_licenses, "initial_licenses", args.full_refresh)
    n_chg = load_source(engine, license_changed, "license_changes", args.full_refresh)

    print(f"data stored! new or changed rows: initial_licenses={n_init}, license_changes={n_chg}")

    if args.checks:

        print("checking initial_licenses")
        print(pd.read_sql(
        "SELECT * FROM initial_licenses LIMIT 10",
        engine
    ))
        print("checking license_chnages")
        print(pd.read_sql(
        "SELECT * FROM license_changes LIMIT 10",
        engine
    ))
        print("checks done!")
