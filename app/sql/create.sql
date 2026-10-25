CREATE TABLE IF NOT EXISTS initial_licenses (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    type VARCHAR(50),
    creation_date DATE,
    price INTEGER,
    renewable BOOLEAN,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS license_changes (
     id INTEGER PRIMARY KEY,
     license_id INTEGER,
     date DATE,
     price INTEGER,
     type VARCHAR(50),
     renewable BOOLEAN,
     ingested_at TIMESTAMPTZ NOT NULL DEFAULT now());

-- Upgrade tables created by an older version (no arrival timestamp, or
-- recreated without a primary key by the old `to_sql(if_exists="replace")` loader).
-- `ingested_at` = when the row arrived in the database; the incremental
-- pipeline uses it as its watermark, so late-arriving events are picked up.
ALTER TABLE initial_licenses ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE license_changes ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ NOT NULL DEFAULT now();

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'initial_licenses'::regclass AND contype = 'p') THEN
        ALTER TABLE initial_licenses ADD PRIMARY KEY (id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'license_changes'::regclass AND contype = 'p') THEN
        ALTER TABLE license_changes ADD PRIMARY KEY (id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS initial_licenses_ingested_at_idx ON initial_licenses (ingested_at);
CREATE INDEX IF NOT EXISTS license_changes_ingested_at_idx ON license_changes (ingested_at);
