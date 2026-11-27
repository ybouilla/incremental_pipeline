
# Data Incremental pipeline showcase

![Pipeline overview: generate, validate, load, transform, incremental loads with watermarks](docs/pipeline_overview.gif)

## 📦  Project Overview

This project simulates a full data lifecycle pipeline:

- 1. Synthetic data generation (license subscriptions)
- 2. Data validation (Pandera, and dbt tests in the database)
- 3. Data transformation (Python/pandas, SQL and dbt)
- 4. Storage in PostgreSQL database (pipeline watermarks in SQLite)
- 5. Incremental pipeline processing
- 6. Full Docker-based orchestration
- 7. Automated testing suite (pytest)

## 0. Installation 

**Requirements**:
- Linux OS or Windows with WSL2 (Windows Subsystem Linux) activated
- Python 3.12 (see `.python-version`) and [uv](https://github.com/astral-sh/uv)
- PostgreSQL, for the database steps (see `app/DBSETUP.md`)
- Docker (optional), only to run the whole pipeline in containers

**Docker installation**, if you want to run the pipeline through Docker:
- [docker installation for linux](https://docs.docker.com/engine/install/)
- [docker installation for windows and wsl2](https://docs.docker.com/desktop/features/wsl/)

### Using uv as a package manager
*Nota*: [uv](https://github.com/astral-sh/uv) has been selected here for its known efficiency. Other python package managers should work but have not been tested.

First install uv. Then create the virtual environment and install the requirements:
**On Linux**:
```shell
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## 1. Data Generation
Run the following script: 
```shell
uv run python -m app.main_data_generation
```
It writes `initial_licenses.csv` and `license_changes.csv` in the `app/csv` folder.

### Parameters
Default parameters are defined in the script `main_data_generation.py`; use `uv run python -m app.main_data_generation --help` to override them.


## 2. Validation step
This step checks that the generated data are coherent, before any further processes.
### Validation script

To run the validation script, enter in a terminal

```shell
uv run python -m app.validate_csv
```
The rules are [Pandera](https://pandera.readthedocs.io) schemas (types, unique ids, valid license types, prices between 0 and 5000) plus two rules across both files (every change points to a known license, no change before a license's creation).
The script exits with code 1 if any rule fails. Output logs are located in the `app/csv` folder, under the name `validation.log`.

The same rules are checked in the database by dbt tests (see [7.2](#72-tests-through-dbt)).

## 3. Transformation step

### Transformation with python/pandas
To run transformation, enter in a terminal
`uv run python -m app.transformation`

The script requires both csv files `initial_licenses.csv` and `license_changes.csv` in the `app/csv` folder.
It outputs `transformed.csv` in the `app/csv` folder.

### Storage in SQL database:
For database usage, we are going to use **PostgreSQL**.

To run the PostgreSQL service locally, use the following (first-time database and user creation: see `app/DBSETUP.md`):
```shell
sudo service postgresql start
```

SQL queries are stored in the `app/sql` folder.
Process:
- `create.sql` creates the source tables (`initial_licenses`, `license_changes`, each with an `ingested_at` arrival timestamp). It can be run alone with `uv run python -m app.create_db`.
- The python script `app/add_database.py` loads the generated csv files into these tables (it also runs `create.sql` first): `uv run python -m app.add_database`. Rows are upserted: new or changed rows get a new `ingested_at`, unchanged rows keep theirs. Use `--full-refresh` to empty the tables before loading.
- `req.sql` (SQL query) mimics the behaviour of `transformation.py`. It is written in the **SQLite** dialect: it gives the same results as `transformation.py` on SQLite, but it does not run on PostgreSQL yet (the `pipeline` Docker mode prints the errors and continues, since `psql` exits with code 0).

## 4. Industrialisation through Docker

To run the one-shot data pipeline with Docker compose (PostgreSQL + pipeline containers):
```shell
docker compose build base
docker compose up --build --abort-on-container-exit --exit-code-from pipeline
```
The pipeline containers mount the local `app/csv` folder, so generated data, transformed data and logs end up there. They run as user `1000:1000` by default (the first user on Linux/WSL), so the files belong to you. If your user id differs, run compose with your own ids, which also go into the image builds:
```shell
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose build base
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up --build --abort-on-container-exit --exit-code-from pipeline
```
The CI does this, since the GitHub runner user is not 1000.

To run the pipeline image alone and keep its data and logs, build it, then mount a local folder:
```shell
docker build -t pipeline .
docker run -v $(pwd)/app/mnt:/app/app/csv -it pipeline 
```
Data and logs can then be accessed in the mounted folder (freshly created): `app/mnt`.
The image needs the `pipeline-base` image (`docker compose build base`) and, for the database steps, a reachable PostgreSQL service named `db`.

End services and delete the database volume:

```shell
docker compose down -v
```

An entrypoint file (`entrypoint.sh`, selected with the `MODE` variable) executes the different elements of the pipeline. In `MODE=pipeline`:
- data generation
- data validation
- sql database creation (PostgreSQL)
- data storage in database
- data transformation (python)
- data transformation (SQL)

Other modes: `test` (pytest), `test-dbt` and `dbt-pipeline` (dbt run and tests). The incremental pipeline has its own image (`Dockerfile.pip`, `entrypoint_pip.sh`, modes `incremental_pipeline` and `incremental_pipeline_backfill`).

The same can be run with explicit build steps and env file:
```shell
MODE=pipeline docker compose --env-file ./app/config/.env.postgre build --no-cache base
MODE=pipeline docker compose --env-file ./app/config/.env.postgre build --no-cache pipeline

MODE=pipeline docker compose --env-file ./app/config/.env.postgre up
```

End docker compose services, and delete the database volume (`-v`):

```shell
docker compose --env-file ./app/config/.env.postgre down -v
```

## 5. Incremental pipeline

### 5.1 Conception

* late-arriving data: the watermark is the **arrival time** of a row (`ingested_at`), not its event date. A late event (old event date, new arrival) is therefore picked up by the next incremental run.
* idempotence: rows are upserted on their primary key (`id`), so re-processing the same rows creates no duplicates.
* failures: rows are processed in arrival order, in batches with retries. If a batch still fails, processing stops and the watermark stays before that batch, so the next run resumes from there.

### 5.2 Incremental pipeline using python

Pipeline state (watermarks) is stored in a SQLite database `app/states/state.db` (table `pipeline_state`). Each source (created licenses, license changes) keeps its own watermark, per storage backend. Writes are transactional, so a crash cannot leave a half-written checkpoint.
To run the incremental pipeline, enter:
`uv run python -m app.incremental_pipeline `

Use `--data-type` to choose where data is read from and written to:
* `--data-type db` (default): reads PostgreSQL tables `initial_licenses` / `license_changes` (load them first with `uv run python -m app.add_database`) and upserts into tables `target_init_license` / `target_chgs` (created if missing). Connection settings come from the `DB_*` environment variables, or `app/config/.env.postgre`.
* `--data-type csv`: reads `app/csv/initial_licenses.csv` / `license_changes.csv` and writes `app/csv/target_init_license.csv` / `target_chgs.csv`. CSV files have no `ingested_at` column, so a file's modification time is used as the arrival time of its rows.

```shell
uv run python -m app.incremental_pipeline --data-type csv
```

Tests for the db backend run only when `PIPELINE_TEST_DB_URL` points at a disposable database (its tables are dropped), e.g. `PIPELINE_TEST_DB_URL=postgresql+psycopg://user:password@localhost:5432/test_db uv run pytest app/tests`.

In the code, an optional extra lookback is set with `LOOKBACK_MINUTES` (default 0): each run then re-reads `watermark - LOOKBACK_MINUTES`.

There is an option to run the backfill pipeline:
```shell
uv run python -m app.incremental_pipeline --mode backfill
```
Backfill re-reads, for each source, the rows that arrived in the `--days` (default 5) before its watermark, up to now. Since late events are already handled by the incremental run, backfill is a repair tool, e.g. after a target table was damaged. The watermark itself never moves backwards.

For further information, type to display help message:
```shell
uv run python -m app.incremental_pipeline --help
```

### 5.3 Late data arrival
The script reproduces an incremental pipeline, with the following characteristics:
* Incremental pipeline: fast, reads only the rows that arrived after the watermark, late events included
* Backfill pipeline: re-applies recent loads to repair targets

Hence, one can run the incremental pipeline every hour, and execute the backfill pipeline when a target needs repairing.


## 6. dbt

Populate the PostgreSQL database:

```shell
sudo service postgresql start
uv run python -m app.add_database --initial-licenses "app/csv/initial_licenses.csv" --license-changes "app/csv/license_changes.csv"
``` 

### dbt queries structure
```
models/
├── staging/licenses/
│   ├── _licenses_sources.yml          (raw tables + their tests)
│   ├── stg_initial_licenses.sql
│   └── stg_license_changes.sql
│
├── intermediate/licenses/
│   ├── inter_state_licenses.sql
│   ├── inter_licenses_calendar.sql
│   ├── inter_licenses_daily_states.sql
│   └── inter_license_daily_metrics.sql
│
└── marts/
    └── mart_license_metrics.sql
tests/
├── generic/value_between.sql
└── assert_change_after_creation.sql
```

To check that the settings are correct, run:
```shell
cd app/dbt
dbt debug
dbt ls
```

and then run!
```shell
dbt clean
dbt run --full-refresh
```
**TODO**: add seeding for csv ?



### dbt workflow

```
initial_licenses (source)            license_changes (source)
        │                                     │
        ▼                                     ▼
stg_initial_licenses                 stg_license_changes
        │                                     │
        ├──────────────► inter_state_licenses ◄┘
        │                     │
        │                     ▼
        │             inter_licenses_calendar
        │                     │
        ▼                     ▼
      inter_licenses_daily_states  (also uses inter_state_licenses)
                    │
                    ▼
      inter_license_daily_metrics
                    │
                    ▼
        mart_license_metrics  (also uses inter_licenses_daily_states
                               and inter_licenses_calendar)
```

## 7. Tests
### 7.1 Tests (pytest)
To run the tests, execute from a terminal:
`uv run pytest -v app/tests`

### 7.2 Tests through dbt
The raw tables are tested as dbt sources, with the same rules as `app/validate_csv.py`. These tests need no `dbt run`:

```shell
cd app/dbt
dbt test --select source:licenses
```

To test the models too:

```shell
cd app/dbt
dbt run  # always perform a dbt run before testing models
dbt test
```

### 7.3 Tests through docker
To launch the test suite through docker, one can use:
```shell
docker compose build base
docker build -t pipeline .
docker run -e MODE=test -it pipeline 
```
**Test structure**:
Several tests are provided:
- **unit tests**: `test_validate.py` (Pandera rules), `test_transformation.py`, `test_state_manager.py` (watermark storage)
- **integration tests**: `test_pipeline.py` executing the whole transformation, `test_incremental_pipeline.py` (CSV backend, and PostgreSQL backend when `PIPELINE_TEST_DB_URL` is set)
- **consistency tests**: `test_data_consistency.py` and `test_data_c.py` check that generated data are consistent, data integrity checks
