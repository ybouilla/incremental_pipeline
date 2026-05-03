import pandas as pd
import sqlite3

## to be used in docker only

# Load CSV
init_licenses = pd.read_csv("/app/app/csv/initial_licenses.csv", header=0, index_col=False)
license_changed = pd.read_csv("/app/app/csv/license_changes.csv", header=0, index_col=False)
# Connect to SQLite
conn = sqlite3.connect("data.db")

# Append into existing table (IMPORTANT: table already exists)
init_licenses.to_sql("initial_licenses", conn, if_exists="append", index=False)

license_changed.to_sql("license_changes", conn, if_exists="append", index=False)

conn.close()