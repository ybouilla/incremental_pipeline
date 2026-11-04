import os
import shutil
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from app import incremental_pipeline as ip
from app.incremental_pipeline_tools import state_manager
from app.incremental_pipeline_tools.state_manager import DEFAULT_PIPELINE, EPOCH_UTC, load_checkpoint

INIT_FILE = "test_initial_licenses.csv"
CHGD_FILE = "test_chg_license.csv"
END = "2030-01-01T00:00:00"
TEST_CSV = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_csv")

# e.g. postgresql+psycopg://user:password@localhost:5432/test_db ; the db tests are skipped without it
TEST_DB_URL = os.getenv("PIPELINE_TEST_DB_URL")


def mtime_utc(path) -> datetime:
    return datetime.fromtimestamp(os.path.getmtime(path), timezone.utc)


@pytest.fixture(autouse=True)
def state_db(tmp_path, monkeypatch):
    monkeypatch.setattr(state_manager, "STATE_DB", os.path.join(tmp_path, "state.db"))
    monkeypatch.setattr(state_manager, "LEGACY_STATE_FILE", os.path.join(tmp_path, "missing.csv"))


@pytest.fixture
def count_upserts(monkeypatch):
    """Count rows written through the csv upsert."""
    real_upsert = ip.upsert_batch
    written = {"rows": 0}

    def counting_upsert(records, target):
        written["rows"] += len(records)
        return real_upsert(records, target)

    monkeypatch.setattr(ip, "upsert_batch", counting_upsert)
    return written


class TestIncrementalPipelineCsv:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        for f in (INIT_FILE, CHGD_FILE):
            shutil.copy(os.path.join(TEST_CSV, f), tmp_path)
        self.folder = str(tmp_path)
        self.chg_path = os.path.join(self.folder, CHGD_FILE)
        self.backend = ip.CsvBackend(self.folder, INIT_FILE, CHGD_FILE, "out_init.csv", "out_chg.csv")

    def run(self):
        ip.run_pipeline(None, END, self.backend)

    def changed_state(self):
        return load_checkpoint(f"{DEFAULT_PIPELINE}.csv.changed")

    def touch(self, path, seconds_later: int):
        t = os.path.getmtime(path) + seconds_later
        os.utime(path, (t, t))

    def test_full_run_sets_per_source_watermarks(self):
        self.run()
        created = load_checkpoint(f"{DEFAULT_PIPELINE}.csv.created")
        # without an ingested_at column, rows arrive at the file's modification time
        assert created["last_processed_timestamp"] == mtime_utc(os.path.join(self.folder, INIT_FILE))
        assert self.changed_state()["last_processed_timestamp"] == mtime_utc(self.chg_path)
        assert len(self.backend.read_target("changed")) == 500

    def test_rerun_skips_untouched_files(self, count_upserts):
        self.run()
        assert count_upserts["rows"] == 510
        self.run()
        assert count_upserts["rows"] == 510  # nothing new arrived
        out = self.backend.read_target("changed")
        assert len(out) == 500 and out["id"].is_unique

    def test_late_event_is_picked_up_by_incremental_run(self):
        self.run()
        changes = pd.read_csv(self.chg_path)
        late = changes.iloc[[0]].copy()
        late["id"] = changes["id"].max() + 1
        late["date"] = "2023-01-02"  # event far behind everything processed so far
        pd.concat([changes, late]).to_csv(self.chg_path, index=False)
        self.touch(self.chg_path, 10)

        self.run()
        out = self.backend.read_target("changed")
        assert int(late["id"].item()) in set(out["id"])
        assert len(out) == 501 and out["id"].is_unique
        assert self.changed_state()["last_processed_timestamp"] == mtime_utc(self.chg_path)

    def test_failed_batch_holds_watermark_before_shared_timestamp(self, monkeypatch):
        real_upsert = ip.upsert_batch
        calls = {"n": 0}

        def flaky_upsert(records, target):
            # changed source: batch 0 succeeds, every later batch fails
            if "license_id" in records.columns:
                calls["n"] += 1
                if calls["n"] > 1:
                    raise RuntimeError("boom")
            return real_upsert(records, target)

        monkeypatch.setattr(ip, "upsert_batch", flaky_upsert)
        self.run()

        # all rows share one arrival time (one file): the watermark stays just before it
        arrived = mtime_utc(self.chg_path)
        state = self.changed_state()
        assert state["last_processed_timestamp"] == arrived - timedelta(microseconds=1)
        assert state["last_data_timestamp"] == arrived

        # next run re-reads the whole load and completes
        monkeypatch.setattr(ip, "upsert_batch", real_upsert)
        self.run()
        assert len(self.backend.read_target("changed")) == 500
        assert self.changed_state()["last_processed_timestamp"] == arrived

    def test_ingested_at_column_is_used_when_present(self):
        changes = pd.read_csv(self.chg_path)
        changes["ingested_at"] = "2024-01-01T00:00:00+00:00"
        changes.loc[changes.index[-5:], "ingested_at"] = "2024-02-01T00:00:00+00:00"
        changes.to_csv(self.chg_path, index=False)

        cut = datetime(2024, 1, 1, tzinfo=timezone.utc)
        src = ip.SOURCES[1]
        assert len(ip.fetch_source_data(self.chg_path, src["date_col"], cut, parse_end(), strict=True)) == 5
        assert len(ip.fetch_source_data(self.chg_path, src["date_col"], cut, parse_end(), strict=False)) == 500

    def test_backfill_reapplies_recent_loads(self, count_upserts):
        self.run()
        watermark = self.changed_state()["last_processed_timestamp"]
        os.remove(os.path.join(self.folder, "out_chg.csv"))  # damaged target

        self.run()
        assert not os.path.exists(os.path.join(self.folder, "out_chg.csv"))  # incremental: nothing new

        ip.run_backfill_pipeline(self.backend, days=1)
        assert len(self.backend.read_target("changed")) == 500
        assert self.changed_state()["last_processed_timestamp"] == watermark


def parse_end():
    return state_manager.parse_ts(END)


@pytest.mark.skipif(not TEST_DB_URL, reason="PIPELINE_TEST_DB_URL not set")
class TestIncrementalPipelineDb:
    """Runs against a disposable database: its source and target tables are dropped."""
    TABLES = ("initial_licenses", "license_changes", "out_init", "out_chg")

    @pytest.fixture(autouse=True)
    def setup(self):
        from app import add_database

        self.add_database = add_database
        # fail fast instead of hanging if a DROP/TRUNCATE waits on a lock held by another session
        self.engine = create_engine(TEST_DB_URL, connect_args={
            "options": "-c timezone=UTC -c lock_timeout=10s -c statement_timeout=60s"})
        self.drop_tables()
        add_database.create_source_tables(self.engine)
        self.init, self.chg = add_database.read_sources(os.path.join(TEST_CSV, INIT_FILE),
                                                        os.path.join(TEST_CSV, CHGD_FILE))
        add_database.load_source(self.engine, self.init, "initial_licenses")
        add_database.load_source(self.engine, self.chg, "license_changes")

        self.backend = ip.PostgresBackend(self.engine, "initial_licenses", "license_changes", "out_init", "out_chg")
        yield
        self.drop_tables()
        self.engine.dispose()

    def drop_tables(self):
        with self.engine.begin() as conn:
            for t in self.TABLES:
                conn.execute(text(f"DROP TABLE IF EXISTS {t}"))

    def run(self):
        ip.run_pipeline(None, END, self.backend)

    def max_ingested(self, table):
        with self.engine.connect() as conn:
            return conn.execute(text(f"SELECT max(ingested_at) FROM {table}")).scalar()

    def test_full_run_and_rerun(self):
        self.run()
        self.run()

        out_init = self.backend.read_target("created")
        out_chg = self.backend.read_target("changed")
        assert len(out_init) == 10 and out_init["id"].is_unique
        assert len(out_chg) == 500 and out_chg["id"].is_unique

        changed = load_checkpoint(f"{DEFAULT_PIPELINE}.db.changed")
        assert changed["last_processed_timestamp"] == self.max_ingested("license_changes")
        # csv backend watermark untouched
        assert load_checkpoint(f"{DEFAULT_PIPELINE}.csv.changed")["last_processed_timestamp"] == EPOCH_UTC

    def test_reload_restamps_only_new_or_changed_rows(self):
        assert self.add_database.load_source(self.engine, self.chg, "license_changes") == 0

        changed = self.chg.copy()
        changed.loc[changed["id"] == 0, "price"] = -1
        assert self.add_database.load_source(self.engine, changed, "license_changes") == 1

    def test_late_event_is_picked_up_by_incremental_run(self):
        self.run()
        first_watermark = load_checkpoint(f"{DEFAULT_PIPELINE}.db.changed")["last_processed_timestamp"]

        late = self.chg.iloc[[0]].copy()
        late["id"] = self.chg["id"].max() + 1
        late["date"] = pd.Timestamp("2023-01-02").date()  # event far in the past, arriving now
        corrected = self.chg[self.chg["id"] == 0].copy()
        corrected["price"] = -1  # correction of an already processed event
        self.add_database.load_source(self.engine, pd.concat([late, corrected]), "license_changes")

        fetched = ip.fetch_source_data_db("license_changes", "date", first_watermark,
                                          parse_end(), True, self.engine)
        assert set(fetched["id"]) == {int(late["id"].item()), 0}

        self.run()
        out = self.backend.read_target("changed")
        assert len(out) == 501 and out["id"].is_unique
        assert out.loc[out["id"] == 0, "price"].item() == -1
        assert load_checkpoint(f"{DEFAULT_PIPELINE}.db.changed")["last_processed_timestamp"] == self.max_ingested("license_changes")
