from datetime import datetime, timezone
import csv
import os
import sqlite3
from app.config import PROJECT_ROOT
from app.logger import logger

dir_path = PROJECT_ROOT
STATE_DB = os.path.join(dir_path, "states", "state.db")
LEGACY_STATE_FILE = os.path.join(dir_path, "states", "state.csv")  # imported once if STATE_DB is new
DEFAULT_PIPELINE = "incremental_licenses"
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)

CREATE_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_state (
    pipeline_name            TEXT PRIMARY KEY,
    last_data_timestamp      TEXT NOT NULL,
    last_processed_timestamp TEXT NOT NULL,
    updated_at               TEXT NOT NULL
)
"""

UPSERT_STATE = """
INSERT INTO pipeline_state (pipeline_name, last_data_timestamp, last_processed_timestamp, updated_at)
VALUES (:pipeline_name, :last_data_timestamp, :last_processed_timestamp, :updated_at)
ON CONFLICT (pipeline_name) DO UPDATE SET
    last_data_timestamp      = excluded.last_data_timestamp,
    last_processed_timestamp = excluded.last_processed_timestamp,
    updated_at               = excluded.updated_at
"""


# ----------------------------
# Helpers
# ----------------------------
def to_utc(dt: datetime) -> datetime:
    """Treat naive datetimes as UTC, convert aware ones to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_ts(ts: str) -> datetime:
    return to_utc(datetime.fromisoformat(ts))


def format_ts(dt: datetime) -> str:
    return to_utc(dt).isoformat()


def _connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(CREATE_STATE_TABLE)
    return conn


def _import_legacy_csv(conn: sqlite3.Connection):
    """Seed the state DB from the old states/state.csv so the watermark is not reset."""
    if not os.path.exists(LEGACY_STATE_FILE):
        return
    with open(LEGACY_STATE_FILE, "r") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return
    row = rows[0]
    with conn:
        conn.execute(UPSERT_STATE, {
            "pipeline_name": DEFAULT_PIPELINE,
            "last_data_timestamp": format_ts(parse_ts(row["last_data_timestamp"])),
            "last_processed_timestamp": format_ts(parse_ts(row["last_processed_timestamp"])),
            "updated_at": format_ts(datetime.now(timezone.utc)),
        })
    logger.info(f"[state] imported legacy checkpoint from {LEGACY_STATE_FILE}")


# ----------------------------
# State (checkpoint)
# checkpoint facility to save when last execution has been performed
# ----------------------------
def load_checkpoint(pipeline_name: str = DEFAULT_PIPELINE, db_path: str = None) -> dict:
    db_path = db_path or STATE_DB
    is_new = not os.path.exists(db_path)
    conn = _connect(db_path)
    try:
        if is_new and db_path == STATE_DB:
            _import_legacy_csv(conn)

        row = conn.execute(
            "SELECT last_data_timestamp, last_processed_timestamp "
            "FROM pipeline_state WHERE pipeline_name = ?",
            (pipeline_name,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return {
            "last_data_timestamp": EPOCH_UTC,
            "last_processed_timestamp": EPOCH_UTC,
        }

    return {
        "last_data_timestamp": parse_ts(row[0]),
        "last_processed_timestamp": parse_ts(row[1]),
    }


def save_checkpoint(last_data_ts: datetime, last_processed_ts: datetime,
                    pipeline_name: str = DEFAULT_PIPELINE, db_path: str = None):
    conn = _connect(db_path or STATE_DB)
    try:
        # `with conn` commits on success and rolls back on error: the write is atomic
        with conn:
            conn.execute(UPSERT_STATE, {
                "pipeline_name": pipeline_name,
                "last_data_timestamp": format_ts(last_data_ts),
                "last_processed_timestamp": format_ts(last_processed_ts),
                "updated_at": format_ts(datetime.now(timezone.utc)),
            })
    finally:
        conn.close()
