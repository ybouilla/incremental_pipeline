import argparse
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, Date
import os

## to be used in docker only

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
    "--checks",
    default=False,
    type=bool,
    help="check if data are stored accordingly",
)
args = parser.parse_args()

current_folder = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_folder, "config", ".env.postgre")) 

init_licenses_path = args.initial_licenses
license_changes_path = args.license_changes
# Load CSV
init_licenses = pd.read_csv(init_licenses_path, header=0, index_col=False)
license_changed = pd.read_csv(license_changes_path, header=0, index_col=False)
# Connect to SQLite
#sql_db_path = args.db
db_host = args.db_host 
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

# Append into existing table (IMPORTANT: table already exists)
init_licenses["creation_date"] = pd.to_datetime(init_licenses["creation_date"]).dt.date
init_licenses.to_sql("initial_licenses", engine, if_exists="replace", index=False, dtype={"creation_date": Date()})

license_changed["date"] = pd.to_datetime(license_changed["date"]).dt.date
license_changed.to_sql("license_changes", engine, if_exists="replace", index=False,
                       dtype={"date": Date()})


print("data stored!")

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