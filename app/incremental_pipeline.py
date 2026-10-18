import argparse
import csv
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from app.config import PROJECT_ROOT
from app.transformation import transform_pipeline
from app.logger import  set_logging, logger
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Dict, Tuple
from app.incremental_pipeline_tools.state_manager import parse_ts, load_checkpoint, save_checkpoint, DEFAULT_PIPELINE
from sqlalchemy import URL, create_engine, text

# ----------------------------
# Config
# ----------------------------


LOOKBACK_MINUTES = 0  # lookback

N_BATCH = 10  # number of batches
MAX_RETRIES = 3  # number of retries to process when a batch of data fails

# Sources processed by the pipeline; each one keeps its own watermark
SOURCES = [
    {"name": "created", "date_col": "creation_date",
     "columns": ["id", "customer_id", "type", "creation_date", "price", "renewable"]},
    {"name": "changed", "date_col": "date",
     "columns": ["id", "license_id", "date", "price", "type", "renewable"]},
]


def get_db_engine():
    """
    Build a SQLAlchemy engine for the PostgreSQL database.

    Connection settings come from DB_HOST, DB_PORT, DB_NAME, DB_USER and
    DB_PASSWORD. Values already set in the environment (e.g. by docker
    compose) win over `app/config/.env.postgre`.
    """
    load_dotenv(os.path.join(PROJECT_ROOT, "config", ".env.postgre"))
    url = URL.create(
        "postgresql+psycopg",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME"),
    )
    # compare and store timestamps in UTC whatever the server's default timezone is
    return create_engine(url, connect_args={"options": "-c timezone=UTC"})


def create_target_tables(db_engine, created_table: str, changed_table: str):
    """Create the target tables (with an `id` primary key, needed for upserts) if missing."""
    with db_engine.begin() as connection:
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {created_table} (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                type VARCHAR(50),
                creation_date DATE,
                price INTEGER,
                renewable BOOLEAN)
        """))
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {changed_table} (
                id INTEGER PRIMARY KEY,
                license_id INTEGER,
                date DATE,
                price INTEGER,
                type VARCHAR(50),
                renewable BOOLEAN)
        """))


def fetch_source_data_db(
    start_ts: datetime,
    end_ts: datetime,
    init_table: str,
    changed_table: str,
    db_engine
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load source datasets from database within a given time window.

    Parameters
    ----------
    start_ts : datetime
        Start of the extraction window (inclusive).
    end_ts : datetime
        End of the extraction window (inclusive).
    init_table : str
        Database table containing initial records.
    changed_table : str
        Database table containing change events.
    db_engine : sqlalchemy.Engine
        Engine connected to the PostgreSQL database.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        Filtered initial dataset and change events dataset.
    """

    logger.info(f"Fetching data from {start_ts} to {end_ts}")

    params = {
        "start_ts": pd.Timestamp(start_ts).tz_convert("UTC").to_pydatetime(),
        "end_ts": pd.Timestamp(end_ts).tz_convert("UTC").to_pydatetime(),
    }

    created_query = text(f"""
        SELECT *
        FROM {init_table}
        WHERE creation_date >= :start_ts
          AND creation_date <= :end_ts
    """)

    changed_query = text(f"""
        SELECT *
        FROM {changed_table}
        WHERE date >= :start_ts
          AND date <= :end_ts
    """)

    with db_engine.connect() as connection:
        created_licenses = pd.read_sql(created_query, connection, params=params)
        changed_licenses = pd.read_sql(changed_query, connection, params=params)

    # Keep the same datetime normalization as the CSV implementation
    created_licenses["creation_date"] = pd.to_datetime(created_licenses["creation_date"], utc=True)
    changed_licenses["date"] = pd.to_datetime(changed_licenses["date"], utc=True)

    logger.info(f"Fetched {len(created_licenses)} records from {init_table}")
    logger.info(f"Fetched {len(changed_licenses)} records from {changed_table}")

    return created_licenses, changed_licenses

def fetch_source_data(start_ts: datetime, end_ts: datetime, init_file: str,
                      changed_file: str, folder_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and filter source datasets within a given time window.

    This function reads raw CSV files, converts timestamp columns to UTC timezone,
    and filters records based on the provided start and end timestamps.

    Parameters
    ----------
    start_ts : datetime
        Start of the extraction window (inclusive).
    end_ts : datetime
        End of the extraction window (inclusive).
    init_file : str
        Filename of the initial dataset.
    changed_file : str
        Filename of the change events dataset.
    folder_name : str
        Directory containing input files.

    Returns
    -------
    created_licenses : pd.DataFrame
        Filtered initial dataset within the time window.
    changed_licenses : pd.DataFrame
        Filtered change events dataset within the time window.
    """
    logger.info(f"Fetching data from {start_ts} to {end_ts}")
        

    created_licenses = pd.read_csv(os.path.join(folder_name, init_file), index_col=False)
    changed_licenses = pd.read_csv(os.path.join(folder_name, changed_file), index_col=False)
    created_licenses["creation_date"] = pd.to_datetime(created_licenses["creation_date"], utc=True)
    changed_licenses["date"] = pd.to_datetime(changed_licenses["date"], utc=True)

    created_licenses = created_licenses[
        (created_licenses["creation_date"] >= pd.Timestamp(start_ts).tz_convert("UTC")) &
        (created_licenses["creation_date"] <= pd.Timestamp(end_ts).tz_convert("UTC"))
    ]
    changed_licenses = changed_licenses[
        (changed_licenses["date"] >= pd.Timestamp(start_ts).tz_convert("UTC")) &
        (changed_licenses["date"] <= pd.Timestamp(end_ts).tz_convert("UTC"))
    ]
    # data = data[
    #     (data["date"] >= pd.Timestamp(start_ts).tz_convert("UTC")) &
    #     (data["date"] <= pd.Timestamp(end_ts).tz_convert("UTC"))
    # ]
    logger.info(f"Fetched {len(created_licenses)} from {init_file} records")
    logger.info(f"Fetched {len(changed_licenses)} from {changed_file} records")
    return created_licenses, changed_licenses

# ----------------------------
# Target store (idempotent)
# ----------------------------
def load_target(folder_name: str, output_file: str, variables: List[str], date_entry: str) -> pd.DataFrame:
    """
    Load the target dataset from storage with optional initialization.

    If the target file does not exist, an empty DataFrame with the
    expected schema is returned.

    Parameters
    ----------
    folder_name : str
        Directory containing output files.
    output_file : str
        Name of the target file.
    variables : List[str]
        Expected column names for the dataset schema.
    date_entry : str
        Column name containing datetime values.

    Returns
    -------
    pd.DataFrame
        Loaded target dataset with parsed datetime column.
    """
    folder = os.path.join(folder_name, output_file)
    if not os.path.exists(folder):
        return pd.DataFrame(columns=variables)

    df = pd.read_csv(folder)
    df[date_entry] = pd.to_datetime(df[date_entry], utc=True)
    return df


def save_target(df: pd.DataFrame, output_file: str, folder_name: str):
    """
    Persist the target dataset to disk as a CSV file.

    Parameters
    ----------
    df : pd.DataFrame
        Dataset to save.
    output_file : str
        Output filename.
    folder_name : str
        Directory where file is saved.
    """
    #print("data saved", df)

    df.to_csv(os.path.join(folder_name, output_file), index=False)


def upsert_batch(records: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    """
    Perform an idempotent upsert of incoming batch into target dataset.

    New records are merged with existing data, and duplicates are removed
    based on the `id` column, keeping the latest occurrence.

    Parameters
    ----------
    records : pd.DataFrame
        Incoming batch of records.
    target : pd.DataFrame
        Existing dataset.

    Returns
    -------
    pd.DataFrame
        Updated dataset after upsert operation.
    """
#     t = target.to_dict()
#     upsert_query = """upsert into {init_table} ({col})
#     values ({values})
# """ # TODO; store query into a seperate file
    if len(records) == 0:
        return target

    # incoming = pd.DataFrame(records)
    # incoming["date"] = pd.to_datetime(incoming["date"], utc=True)

    # combine old + new
    
    combined = pd.concat([target, records], ignore_index=True)
    combined = combined.drop_duplicates(subset="id", keep="last")  # remove duplicates if any

    return combined

def upsert_batch_db(records: pd.DataFrame, name: str, columns: List[str], db_engine):
    """
    Perform an idempotent upsert of incoming batch into a database table.

    Rows are inserted with `INSERT ... ON CONFLICT (id) DO UPDATE`, so
    re-running a batch overwrites the same rows instead of duplicating them.
    The whole batch is written in one transaction.

    Parameters
    ----------
    records : pd.DataFrame
        Incoming batch of records.
    name : str
        Target table name (must have a primary key on `id`).
    columns : List[str]
        Columns to write.
    db_engine : sqlalchemy.Engine
        Engine connected to the PostgreSQL database.
    """
    if len(records) == 0:
        return

    batch = records[columns]
    # native python values (None for missing) so the driver can adapt them
    rows = batch.astype(object).where(batch.notna(), None).to_dict(orient="records")

    column_names = ", ".join(columns)
    value_names = ", ".join(f":{col}" for col in columns)
    update_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in columns if col != "id")

    upsert_query = text(f"""
        INSERT INTO {name} ({column_names})
        VALUES ({value_names})
        ON CONFLICT (id)
        DO UPDATE SET
            {update_clause}
    """)

    with db_engine.begin() as connection:
        connection.execute(upsert_query, rows)
    logger.info(f"[incremental pipeline] Upserted {len(rows)} records into {name}")

# ----------------------------
# Batch helper
# ----------------------------
def chunk(data: pd.DataFrame, size: int) -> list:
    """
    Split a dataset into fixed-size batches.

    Parameters
    ----------
    data : List[Dict]
        Input dataset.
    size : int
        Batch size.

    Returns
    -------
    list
        List of tuples (batch_id, batch_data).
    """
    return [(i // size, data[i:i + size]) for i in range(0, len(data), size)]
        



# ----------------------------
# Storage backends
# ----------------------------
class CsvBackend:
    """Read sources from CSV files and write targets as CSV files."""
    name = "csv"

    def __init__(self, folder_name: str, inital_license_file: str, changed_license_file: str,
                 created_output: str, changed_output: str):
        self.folder_name = folder_name
        self.input_files = (inital_license_file, changed_license_file)
        self.outputs = {"created": created_output, "changed": changed_output}

    def fetch(self, start_ts: datetime, end_ts: datetime) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return fetch_source_data(start_ts, end_ts, *self.input_files, self.folder_name)

    def writer(self, source: dict) -> Callable[[pd.DataFrame], None]:
        output_file = self.outputs[source["name"]]
        target = load_target(self.folder_name, output_file, source["columns"], source["date_col"])

        def write(batch: pd.DataFrame):
            nonlocal target
            updated = upsert_batch(batch, target).sort_values(source["date_col"])
            save_target(updated, output_file, self.folder_name)
            target = updated  # only keep the new state once it is on disk

        return write

    def read_target(self, source_name: str) -> pd.DataFrame:
        return pd.read_csv(os.path.join(self.folder_name, self.outputs[source_name]), index_col=False)


class PostgresBackend:
    """Read sources from PostgreSQL tables and upsert into target tables."""
    name = "db"

    def __init__(self, db_engine, init_table: str, changed_table: str,
                 created_output: str, changed_output: str):
        self.db_engine = db_engine
        self.input_tables = (init_table, changed_table)
        self.outputs = {"created": created_output, "changed": changed_output}
        create_target_tables(db_engine, self.outputs["created"], self.outputs["changed"])

    def fetch(self, start_ts: datetime, end_ts: datetime) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return fetch_source_data_db(start_ts, end_ts, *self.input_tables, self.db_engine)

    def writer(self, source: dict) -> Callable[[pd.DataFrame], None]:
        table = self.outputs[source["name"]]
        return lambda batch: upsert_batch_db(batch, table, source["columns"], self.db_engine)

    def read_target(self, source_name: str) -> pd.DataFrame:
        with self.db_engine.connect() as connection:
            return pd.read_sql(text(f"SELECT * FROM {self.outputs[source_name]}"), connection)


def make_backend(data_type: str, created_output: str, changed_output: str):
    """
    Build the storage backend selected by `--data-type`.

    csv: sources `initial_licenses.csv` / `license_changes.csv` in `app/csv`,
         targets written to `<output>.csv` in the same folder.
    db:  sources tables `initial_licenses` / `license_changes` in PostgreSQL,
         targets upserted into tables `<output>`.
    """
    if data_type == "db":
        return PostgresBackend(get_db_engine(), "initial_licenses", "license_changes",
                               created_output, changed_output)
    return CsvBackend(os.path.join(PROJECT_ROOT, "csv"), "initial_licenses.csv", "license_changes.csv",
                      f"{created_output}.csv", f"{changed_output}.csv")


# ----------------------------
# Retry batch processing
# ----------------------------
def process_batch(batch_id: int, batch: pd.DataFrame,
                  write: Callable[[pd.DataFrame], None]) -> bool:
    """
    Process and persist a single batch with retry logic.

    This function attempts to write a batch with the backend's writer
    and retries on failure up to MAX_RETRIES.

    Parameters
    ----------
    batch_id : int
        Identifier of the batch.
    batch : pd.DataFrame
        Incoming batch data.
    write : Callable[[pd.DataFrame], None]
        Idempotent writer persisting the batch (see `CsvBackend.writer`,
        `PostgresBackend.writer`).

    Returns
    -------
    success : bool
        Whether the batch was processed successfully.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Batch {batch_id} | attempt {attempt} | size {len(batch)}")
            write(batch)
            logger.info(f"Batch {batch_id} SUCCESS")
            return True

        except Exception as e:
            # change Exception to another exception
            logger.warning(f"Batch {batch_id} failed attempt {attempt}: {e}")

    logger.error(f"Batch {batch_id} FAILED permanently")
    return False


def run_backfill_pipeline(backend, days: int=5):

    """
    Execute a backfill pipeline for late-arriving data.

    This function computes a lookback window and reprocesses recent data
    to ensure consistency in the target datasets.

    Parameters
    ----------
    backend : CsvBackend | PostgresBackend
        Storage backend to read from and write to.
    days : int, optional
        Number of days to backfill (default is 5).

    Returns
    -------
    None
    """

    now = datetime.now(timezone.utc)

    start_ts = now - timedelta(days=days)


    logger.info(f"BACKFILL START | window={start_ts} -> {now}")

    run_pipeline(start_ts, None, backend)

    logger.info("BACKFILL COMPLETE")

# ----------------------------
# Pipeline
# ----------------------------
def process_source(source_name: str, records: pd.DataFrame, date_col: str,
                   write: Callable[[pd.DataFrame], None],
                   state: dict) -> Tuple[datetime, datetime, int]:
    """
    Process one source in timestamp order and compute its new watermarks.

    Records are sorted by `date_col` before batching, so the processed
    watermark only ever covers rows that were actually persisted. Processing
    stops at the first failed batch: the processed watermark stays at the
    last successful batch and the next run picks up from there.

    Parameters
    ----------
    source_name : str
        Name used for logging.
    records : pd.DataFrame
        Records fetched for this source's window.
    date_col : str
        Column holding the event timestamp.
    write : Callable[[pd.DataFrame], None]
        Backend writer for this source.
    state : dict
        Checkpoint loaded for this source.

    Returns
    -------
    max_data_ts : datetime
        Latest timestamp seen in the source.
    max_processed_ts : datetime
        Latest timestamp of contiguously persisted data.
    failed_batches : int
        1 if processing stopped on a failed batch, else 0.
    """
    max_data_ts = state["last_data_timestamp"]
    max_processed_ts = state["last_processed_timestamp"]

    records = records.sort_values(date_col, kind="stable")
    if len(records):
        max_data_ts = max(max_data_ts, records[date_col].max().to_pydatetime())

    for batch_id, batch in chunk(records, N_BATCH):
        logger.info(f"[{source_name}] Batch {batch_id} | size={len(batch)}")
        if not process_batch(batch_id, batch, write):
            logger.error(f"[{source_name}] stopping at batch {batch_id} | processed_ts held at {max_processed_ts}")
            return max_data_ts, max_processed_ts, 1
        max_processed_ts = max(max_processed_ts, batch[date_col].max().to_pydatetime())

    return max_data_ts, max_processed_ts, 0


def run_pipeline(start: str, end: str, backend) :

    """
    Execute the full incremental ingestion pipeline.

    This pipeline:
    1. Loads checkpoint state (one watermark per source and backend)
    2. Defines each source's processing window
    3. Fetches source data
    4. Opens a writer on each target
    5. Processes each source in timestamp-ordered batches with retries
    6. Persists each source's checkpoint

    Parameters
    ----------
    start : str
        Start timestamp (or None to use watermark).
    end : str
        End timestamp (or None for current time).
    backend : CsvBackend | PostgresBackend
        Storage backend to read from and write to (see `make_backend`).

    Returns
    -------
    None
    """
    logger.info(f"PIPELINE START | backend={backend.name}")

    if isinstance(start, str):
        start = parse_ts(start)

    sources = [dict(src) for src in SOURCES]

    # ----------------------------
    # 1-2. Load state and define windows
    # ----------------------------
    end_ts = datetime.now(timezone.utc) if end is None else parse_ts(end)
    for src in sources:
        # csv and db targets are filled independently, so they track separate watermarks
        src["checkpoint"] = f"{DEFAULT_PIPELINE}.{backend.name}.{src['name']}"
        src["state"] = load_checkpoint(src["checkpoint"])
        src["start_ts"] = (start or src["state"]["last_processed_timestamp"]) - timedelta(minutes=LOOKBACK_MINUTES)
        logger.info(f"[{src['name']}] Loaded state | data_ts={src['state']['last_data_timestamp']} "
                    f"| processed_ts={src['state']['last_processed_timestamp']} "
                    f"| window={src['start_ts']} -> {end_ts}")

    # ----------------------------
    # 3. Fetch data (widest window, then narrowed per source)
    # ----------------------------
    fetch_start = min(src["start_ts"] for src in sources)
    sources[0]["records"], sources[1]["records"] = backend.fetch(fetch_start, end_ts)

    failed_batches = 0
    for src in sources:
        records = src["records"]
        records = records[records[src["date_col"]] >= src["start_ts"]]

        # ----------------------------
        # 4-5. Batch processing
        # ----------------------------
        max_data_ts, max_processed_ts, failed = process_source(
            src["name"], records, src["date_col"], backend.writer(src), src["state"])
        failed_batches += failed

        # ----------------------------
        # 6. Persist state
        # ----------------------------
        save_checkpoint(last_data_ts=max_data_ts, last_processed_ts=max_processed_ts,
                        pipeline_name=src["checkpoint"])
        logger.info(f"[{src['name']}] data_ts={max_data_ts} | processed_ts={max_processed_ts}")

    logger.info(f"PIPELINE END | failed_batches={failed_batches}")


if __name__ == "__main__":
    set_logging("incremental.log")

    parser = argparse.ArgumentParser()

    parser.add_argument("--mode", choices=["incremental", "backfill", "test"], default="incremental")
    parser.add_argument("--days", type=int, default=5)
    parser.add_argument("--data-type", choices=["db", "csv"], default="db",
                        help="csv: read/write CSV files in app/csv | "
                             "db: read/write PostgreSQL tables (settings from DB_* env vars or app/config/.env.postgre)")
    args = parser.parse_args()

    if args.mode == "incremental":
        run_pipeline(None, None, make_backend(args.data_type, "target_init_license", "target_chgs"))

    elif args.mode == "backfill":
        run_backfill_pipeline(make_backend(args.data_type, "target_init_license", "target_chgs"),
                              days=args.days)

    elif args.mode == "test":
        #########################################
        # this section is reserved for testing
        start, end = "2023-04-25T10:00:00", "2023-04-30T10:10:00"
        run_pipeline(start, end, make_backend(args.data_type, "target_init_license", "target_chgs"))

        start, end = "2023-04-25T10:00:00", "2023-05-10T10:10:00"
        test_backend = make_backend(args.data_type, "target1", "target2")
        run_pipeline(start, end, test_backend)
        chck = test_backend.read_target("changed")
        assert len(chck["id"]) == len(chck["id"].unique())
