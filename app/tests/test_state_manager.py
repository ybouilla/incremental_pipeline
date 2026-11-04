import os
import sqlite3
from datetime import datetime, timezone, timedelta

from app.incremental_pipeline_tools.state_manager import (
    EPOCH_UTC, load_checkpoint, save_checkpoint, parse_ts,
)


class TestStateManager:
    def test_empty_db_returns_epoch(self, tmp_path):
        db = os.path.join(tmp_path, "state.db")
        state = load_checkpoint(db_path=db)
        assert state["last_data_timestamp"] == EPOCH_UTC
        assert state["last_processed_timestamp"] == EPOCH_UTC

    def test_roundtrip_and_overwrite(self, tmp_path):
        db = os.path.join(tmp_path, "state.db")
        t1 = datetime(2023, 4, 25, 10, tzinfo=timezone.utc)
        t2 = datetime(2023, 4, 26, 10, tzinfo=timezone.utc)
        save_checkpoint(t1, t1, db_path=db)
        save_checkpoint(t2, t1, db_path=db)

        state = load_checkpoint(db_path=db)
        assert state["last_data_timestamp"] == t2
        assert state["last_processed_timestamp"] == t1

        # upsert keeps a single row per pipeline
        with sqlite3.connect(db) as conn:
            assert conn.execute("SELECT COUNT(*) FROM pipeline_state").fetchone()[0] == 1

    def test_pipelines_are_isolated(self, tmp_path):
        db = os.path.join(tmp_path, "state.db")
        t = datetime(2023, 4, 25, tzinfo=timezone.utc)
        save_checkpoint(t, t, pipeline_name="a", db_path=db)
        assert load_checkpoint(pipeline_name="b", db_path=db)["last_data_timestamp"] == EPOCH_UTC

    def test_non_utc_input_is_converted(self, tmp_path):
        db = os.path.join(tmp_path, "state.db")
        paris = timezone(timedelta(hours=2))
        save_checkpoint(datetime(2023, 4, 25, 12, tzinfo=paris),
                        datetime(2023, 4, 25, 12, tzinfo=paris), db_path=db)
        assert load_checkpoint(db_path=db)["last_data_timestamp"] == datetime(2023, 4, 25, 10, tzinfo=timezone.utc)

    def test_parse_ts_keeps_offset(self):
        assert parse_ts("2023-04-25T12:00:00+02:00") == datetime(2023, 4, 25, 10, tzinfo=timezone.utc)
        assert parse_ts("2023-04-25T12:00:00") == datetime(2023, 4, 25, 12, tzinfo=timezone.utc)
