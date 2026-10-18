
# Data Incremental pipeline showcase

## 📦  Project Overview

This project simulates a full data lifecycle pipeline:

- 1. Synthetic data generation (license subscriptions)
- 2. Data validation
- 3. Data transformation (Python + SQL)
- 4. Storage in SQLite database
- 5. Incremental pipeline processing
- 6. Full Docker-based orchestration
- 7. Automated testing suite (pytest)

## 0. Installation 

**Requirements**:
- Linux OS 
- Windows with WSL2 (Windows Subsystem Linux) activated
- Docker
**(optional: docker installation)**
If you want to run the whole pipeline through docker, you may need to install docker on your system.

**Please visit docker installation beforehand**:
- [docker installation for linux](https://docs.docker.com/engine/install/)
- [docker installation for windows and wsl2](https://docs.docker.com/desktop/features/wsl/)

### Using uv as a package manager
*Nota*: [uv](https://github.com/astral-sh/uv) has been selected here for its known efficiency. Others python package managers should be working but have not been tested.

First install uv. Then you can install requirements by running:
**On Linux**:
```
source .venv/bin/activate
uv pip install -r requirements.txt
```

## 1. Data Generation
Run the following script: 
```shell
uv run python -m app.main_data_generation
```
Use `uv run python -m app.main_data_generation --help` for more details.
## Parameters
Parameters are defined in the script `main_data_generation.py`


## 2.1 Validation step
This step checks if data are well generated and are coherent, before any further processes.
### Validation script

To run validation script; enter in a terminal

```shell
uv run python -m app.validate_csv
```
Output logs are located in the `app/csv` folder, under the name `incremental.log`.

## 3. Transformation step

### Transformation with python/pandas
To run transformation, enter in a terminal
`uv run python -m app.transformation`

The script requieres both csv files `initial_licenses.csv`and `license_changes.csv` in `csv`folder
It outputs `transformed.csv` in the `csv` folder.

### Storage in SQL database:
For database usage, we are going to use **PostgreSQL**.

PostgreSQL provides a way to serve through Docker

To run postgre service, use the following:
```shell
sudo service postgresql start
```

SQL queries are stored in `app/sql` folder.
Process:
- `create.sql` is used to create the database
- The python script `app/add_database.py` can be used to load data generated from the csv files inside databases. For that one can run `uv run python -m app.create_db`
- `req.sql` (SQL query) is used to mimick the behaviour of transformation.py but whithin the docker file. 

## 4. Industrialisation and testing steps : Docker and Tests

### Industrialisation through Docker
To use the pipeline within docker, run this command: 
First build & run, using 
```shell
docker compose build base
docker compose up --build --abort-on-container-exit --exit-code-from pipeline
```
(one shot data pipeline)

Second, run docker using 
```shell
docker run -v $(pwd)/app/mnt:/app/app/csv -it pipeline 
```
End services:

```shell
docker compose down -v
```

Data and logs can be accessed using mounted folder (freshly created): `app/mnt`
An entrypoint file has been made in order to execute the different element of the pipeline, ie:
- sql database (made using PostgreSQL)
- data generation
- data validation
- data storage in database
- data transformation (python)
- data transformation (SQL)

## 5. Pipeline (not shipped in docker)

### 5.1 Conception

* late-arriving data: lookback window (last_processed_timestamp - window), ensuring that delayed records are still captured,  to make sure no data is  missed
* idempotence:  idempotent is made through upserts or deduplication based on a primary key.

### 5.2 Incremental pipeline using python

Pipeline state (watermarks) is stored in a SQLite database `app/states/state.db` (table `pipeline_state`). Each source (created licenses, license changes) keeps its own watermark, per storage backend. Writes are transactional, so a crash cannot leave a half-written checkpoint.
To run the incremental pipeline, enter:
`uv run python -m app.incremental_pipeline `

Use `--data-type` to choose where data is read from and written to:
* `--data-type db` (default): reads PostgreSQL tables `initial_licenses` / `license_changes` (load them first with `uv run python -m app.add_database`) and upserts into tables `target_init_license` / `target_chgs` (created if missing). Connection settings come from the `DB_*` environment variables, or `app/config/.env.postgre`.
* `--data-type csv`: reads `app/csv/initial_licenses.csv` / `license_changes.csv` and writes `app/csv/target_init_license.csv` / `target_chgs.csv`.

```shell
uv run python -m app.incremental_pipeline --data-type csv
```

Tests for the db backend run only when `PIPELINE_TEST_DB_URL` points at a disposable database (its tables are dropped), e.g. `PIPELINE_TEST_DB_URL=postgresql+psycopg://user:password@localhost:5432/test_db uv run pytest app/tests`.

In the code, pipeline 's lookback is set using `LOOKBACK_MINUTES`. 
A lookback window is used: `last_processed_date - window`

There is an option to run backfill pipeline:
```shell
uv run python -m app.incremental_pipeline --mode backfill
```
Backfill strategy should be run from time to time, to ensure there are no very late batches of data.
It reprocesses each source from its own watermark minus `--days` (default 5) up to now, so it works on historical data and after a stalled pipeline. The watermark itself never moves backwards.

For further information, type to display help message:
```shell
uv run python -m app.incremental_pipeline --help
```

### 5.3 Late data arrival
I made a script that reproduce an incremental pipeline, with the following characteristics:
* Incremental pipeline: fast, includes a rolling reprocessing window to absorb late data
* Backfill pipeline: ensures correctness, catches very late data like 5-day late data, fixes missed updates

Hence, one can run the incremental pipeline every hours, and execute the backfill pipeline when needed (eg when data arrive late).


## Orchestration through Docker compose

Use:
```shell
MODE=pipeline docker compose --env-file ./app/config/.env.postgre build --no-cache base
MODE=pipeline docker compose --env-file ./app/config/.env.postgre build --no-cache pipeline

MODE=pipeline docker compose --env-file ./app/config/.env.postgre  up
```

End docker compose services

```shell
docker  compose --env-file ./app/config/.env.postgre down
docker volume rm chgmaps_postgres_data
```

## DBT addition :

Populate Postgresql database

```shell
sudo service postgresql start
uv run python -m app.create_db  # create db with sql/creqte.sql queries
uv run python -m app.add_database --initial-licenses "app/csv/initial_licenses.csv" --license-changes "app/csv/license_changes.csv"
``` 

### dbt queries strucutre
```
models/
├── staging/
│   ├── stg_license_changes.sql
│   └── stg_initial_licenses.sql
│
├── intermediate/
│   ├── int_license_states.sql
│   ├── int_license_calendar.sql
│   ├── int_license_daily_states.sql
│   └── int_license_daily_metrics.sql
│
└── marts/
    └── mart_license_metrics.sql
```

First, run dbt dependencies
```shell
cd app/dbt
dbt deps
dbt compile

```
To see if settings are correct, run to check:
```shell
cd app/dbt
dbt ls
dbt debug
```

and then run!
```shell
dbt clean
dbt run --full-refresh
```
**TODO**: add seeding for csv ?



### dbt workflow

```
license_changes
        │
        ▼
stg_license_changes

initial_licenses
        │
        ▼
stg_initial_licenses
        │
        ├─────────────► int_license_states
        │                     │
        │                     ▼
        │             int_license_daily_states
        │                     │
        │                     ▼
        │             int_license_daily_metrics
        │                     │
        ▼                     ▼
     calendar ─────────► mart_license_metrics
```
### 6. Tests (pytest)
#### 6.1 Tests 
To run the test, kindly execute from a terminal:
`uv run pytest -v app/tests`

#### 6.2 Tests through dbt
To test dbt, run:

```shell
cd app/dbt
dbt run  # always perform a dbt run before doing tests
dbt test
```

#### 6.3 Tests through docker
To launch test suit through docker; one can use:
```shell
docker build -t pipeline .
docker run -e MODE=test -it pipeline 
```
**Test structure**:
Several tests are provided:
- **unit tests**: `test_validate.py` and `test_transformation.py`
- **integration tests**: `test_pipeline.py` executing the whole pipeline
- **consistency tests**: `test_data_consistency.py`checks if generated data are consistent, data integrity checks

