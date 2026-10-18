<<<<<<< HEAD

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



## TODO: 

3. dockerize the whole pipeline

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
For database usage, we are going to use **SQlite**.

SQL queries are stored in `app/sql` folder.
Process:
- `create.sql` is used to create the database
- The python script `app/add_database.py` can be used to load data generated from the csv files inside databases.
- `req.sql` (SQL query) is used to mimick the behaviour of transformation.py but whithin the docker file. 

## 4. Industrialisation and testing steps : Docker and Tests

### Industrialisation through Docker
To use the pipeline within docker, run this command: 
First build, using 
```shell
docker build -t pipeline .
```
Second, run docker using 
```shell
docker run -v $(pwd)/app/mnt:/app/app/csv -it pipeline 
```

Data and logs can be accessed using mounted folder: `app/mnt`
An entrypoint file has been made in order to execute the different element of the pipeline, ie:
- sql database (made using SQlite)
- data generation
- data validation
- data storage in database
- data transformation (python)
- data transformation (SQL)

## 5. Pipeline (not shipped in docker)

### 5.1 Conception

* late-arriving data: lookback window(last_processed_timestamp - window), ensuring that delayed records are still captured,  to make sure no data is  missed
* idempotence:  idempotent is made through upserts or deduplication based on a primary key.

### 5.2 Incremental pipeline using python

For simplicity sake; i will save states in a csv file `states/state.csv`, but this would be not suitable for production.
To run the incremental pipeline, enter:
`run uv run python -m app.incremental_pipeline `

In the code, pipeline 's lookback is set using `LOOKBACK_MINUTES`. 
A lookback window is used: `last_processed_date - window`

There is an option to run backfill pipeline:
```shell
uv run python -m app.incremental_pipeline --mode backfill
```
Backfill strategy should be run from time to time, to ensure there are no very late batches of data.

For further information, type to display help message:
```shell
uv run python -m app.incremental_pipeline --help
```

### 5.3 Late data arrival
I made a script that reproduce an incremental pipeline, with the following characteristics:
* Incremental pipeline: fast, includes a rolling reprocessing window to absorb late data
* Backfill pipeline: ensures correctness, catches very late data like 5-day late data, fixes missed updates

Hence, one can run the incremental pipeline every hours, and execute the backfill pipeline when needed (eg when data arrive late).

### 6. Tests (pytest)
#### 6.1 Tests 
To run the test, kindly execute from a terminal:
`uv run pytest -v app/tests`

#### 6.2 Tests through docker
To launch test suit through docker; one can use:
```shell
docker run -e MODE=test -it pipeline 
```
**Test structure**:
Several tests are provided:
- **unit tests**: `test_validate.py` and `test_transformation.py`
- **integration tests**: `test_pipeline.py` executing the whole pipeline
- **consistency tests**: `test_data_consistency.py`checks if generated data are consistent, data integrity checks


## TODO: 

3. dockerize the whole pipeline
4. use uv toml project instead of requirements.txt
=======

# Exercise Chargemaps

## 📦  Project Overview

This project simulates a full data lifecycle pipeline:

- Synthetic data generation (license subscriptions)
- Data validation
- Data transformation (Python + SQL)
- Storage in SQLite database
- Incremental pipeline processing
- Full Docker-based orchestration
- Automated testing suite (pytest)


## 0. Installation 

**Requirements**:
- Linux OS 
- Windows with WSL2 (Windows Subsystem Linux) activated
- Docker

### Using uv as a package manager
*Nota*: [uv](https://github.com/astral-sh/uv) has been selected here for its known efficiency. Others python package managers should be working but have not been tested.

First install uv. Then you can install requirements by running:
**On Linux**:
```
source .venv/bin/activate
uv pip install -r requirements.txt
```

Run the following script: 
```shell
uv run python -m app.main_data_generation
```
Use `uv run python -m app.main_data_generation --help` for more details.

## 1. Data Generation
## Parameters
Parameters are defined in the script `main_data_generation.py`

### a. Number of Licenses

Since the license ids range from 1 to 100, there are a maximum of 100 licenses.
For customer ids, there should be less customers than the number of licenses available. 

### b. Number of licenses per clients
As asked, a paretto distribution is used to model number of licenses per clients. `alpha` has been selected in a way the majority of clients have one or 2 licenses, but a few of them hold way more licenses.

### c. Creation date

To generate creation date; we used a probability to create a date belonging to first quarter of the year. Also we include the fact the date doesnot appear on a weekend (assuming its mainly companies which subscribe).

### d. Changes in the licenses.

To simulate the fact most clients don't change their subscriptions, but only a small portion do, i used a Paretto like distribution, to select which clients will have a lot of modifications.
It has been asked to generate over than 10,000 modifications. 

### e. **Relationship between type and prices:**

Since we have 3 kinds of type (SIM, PASS, SUPERVISION), for simplification sake I proposed to map for each type a price:
 - PASS: 100
 - SIM: 1000
 - SUPERVISION: 5000

###  f.type modelling

I used a Markov Chain to represent the transition dynamics. I defined a 6-state system represented by a single 6×6 transition matrix, where each row defines the probabilities of transitioning between states.

🧩 Conceptual model

This system behaves like:

* A 3-state operational process (core system)
* Embedded in a 6-state Markov chain
With transient “renewal” events acting as instant switches


### g. Renewable:
💡 Key idea
Renewal states do not change the observed state, but they represent meaningful internal transitions in the underlying stochastic process.
Here we assume that when licenses are created, all licenses are renewbale (activated) by default. Then, using the Markov chain, we modelise some states as renewable or not renewable, so the license can be deactivated and then reactivated or changed to another subscription (still staying inactive).

### h. **Possible improvments:**
I am keeping things simple here; but we may consider lower price for customers who has several licenses (like discounts): the more customers have licenses, the more they get some discount from the inital price. Also the longer they have subscribed, the more discount they can get. 

Ideas for even more realistic data:
- discount on number of years customers have been subscribing (renew=True)
- discount on number of license customers are currently subscribing (renew=True)

We can also consider that some licenses could be shared amongst clients. 


## 2.1 Validation step

### Validation script

To run validation script; enter in a terminal

```shell
uv run python -m app.validate_csv
```
Output logs are located in the `app/csv` folder.

## 2.2 Transformation step

### Transformation with python/pandas
To run transformation, enter in a terminal
`uv run python -m app.transformation`

The script requieres both csv files `initial_licenses.csv`and `license_changes.csv` in `csv`folder
It outputs `transformed.csv` in the `csv` folder.

**Details**:

1. **build states** =(concatentaion of init and changed states):
    - cleans created and changed csv data: **noramlizes data**: having the same columns named the same way accross initial and changed, takes only last changed value in `changed_clean`
    - creates `initial_states`, first by considering only created. Its  index is using ["license_id", "date"] 
    - states are a concatenation of clean created and clean changed. I added id=-1 if there are creation but no changes in the states


2. **build_full_daily_states**:
     - creates all_dates: date from min(date) and max(date), and `full_grid` a grid date * licenses. Then performs a left join on creation_date so we have date, license_id and creation_date. we also make sure that dates are after creation_date.
    - then do left join on full_grid and states on [license_id, date]
    - group by ["renewable", "type", "price"], so we have for each entry its type/renewable and associated price
    - returns a df with all dates, license_id and its corresponding [ "type"]
### Storage in SQL database:
For database usage, we are going to use SQlite as a sql database in the docker image. The idea is to keep docker image as small as possible.

SQL queries are stored in `app/sql` folder.
Process:
- `create.sql` is used to create the database
- The python script `app/add_database.py` can be used to load data generated from the csv files inside databases.
- `req.sql` is used to mimick the behaviour of transformation.py but whithin the docker file. 

### Transformation with SQL queries
Please see answer in the file named : `app/sql/req.sql`
**Regarding question about database architecture.**
First, regarding the database creation, it could be nice to inculde a foreign key referring to initial_licenses 's `id`. 
Second; the transformation script is overly large and complex. It could be improved by splitting it into modular steps and using intermediate tables/views instead of relying heavily on Common Table Expressions (CTEs).

## 3. Industrialisation and testing steps : Docker and Tests

### Industrialisation through Docker

**Please visit docker installation beforehand:
- [docker installation for linux](https://docs.docker.com/engine/install/)
- [docker installation for windows and wsl2](https://docs.docker.com/desktop/features/wsl/)

To use the pipeline within docker, run this command: 
First build, using 
```shell
docker build -t pipeline .
```
Second, run docker using 
```shell
docker run -v $(pwd)/app/mnt:/app/app/csv -it pipeline 
```

Data and logs can be accessed using mounted folder: `app/mnt`
An entrypoint file has been made in order to execute the different element of the pipeline, ie:
- sql database (made using SQlite)
- data generation
- data validation
- data storage in database
- data transformation (python)
- data transformation (SQL)


### Tests (pytest)
To run the test, kindly execute from a terminal:
`uv run pytest -v app/tests`

To launch test suit through docker; one can use:
```shell
docker run -e MODE=test -it pipeline 
```
**Test structure**:
Several tests are provided:
- **unit tests**: `test_validate.py` and `test_transformation.py`
- **integration tests**: `test_pipeline.py` executing the whole pipeline
- **consistency tests**: `test_data_consistency.py`checks if generated data are consistent, data integrity checks


### Question CI/CD:
In order to execute CI using a tool like Github action:
1. Continuous Integration (CI)
* Define the OS environment (e.g., Ubuntu)
* Install dependencies using uv
* Run code quality checks (linting, formatting, optional type checking)
* Run and check the documentation generation
* Execute unit, integration,  consistency and end2end tests 
* Validate the full pipeline execution using Docker to ensure reproducibility
* define the workflows configuration file, ie which actions trigger when CI runs (each push request, when pushing on master branch, when releasing)

2. Continuous Delivery / Deployment (CD)
* Build a Docker image of the pipeline
* Deploy or execute the pipeline 

## 4. Pipeline

### 4.1 Conception questions

Regarding data that has been already processed: I would implement an incremental pipeline using a checkpoint (high-water mark / date of new incoming batch of data), typically based on a timestamp like updated_at or an ingestion time.

To handle late-arriving data, I would use a lookback window: each run would reprocess data from last_processed_timestamp - window, ensuring that delayed records are still captured,  so I am sure I won't miss any data, even if I have to re process  data records that I have already processed. I could define a watermark or delay threshold to indicate when data is considered complete (which can better scale to continuous data processing)


I would persist this state in a durable store such as a database table. That way, I can keep track of incoming data, and see which data has been processed and which has been newly added.

Since this causes some reprocessing, I would make the pipeline idempotent by using upserts or deduplication based on a primary key.

This approach ensures I don’t miss late data while keeping the pipeline efficient and recoverable.


### 4.2 Incremental pipeline using python

For simplicity sake; i will save states in a csv file `states/state.csv`, but this would be not suitable for production.
To run the incremental pipeline, enter:
`run uv run python -m app.incremental_pipeline `

In the code, pipeline 's lookback is set using `LOOKBACK_MINUTES`. Since we want to process striclty only new incoming data, I will set `LOOKBACK_MINUTES=0` (ie deactivated), but this is risky especially if data arrive late.  one should set it to a small window (eg 30 minutes).
A lookback window is used: `last_processed_date - window`

There is an option to run backfill pipeline:
```shell
uv run python -m app.incremental_pipeline --mode backfill
```
Backfill strategy should be run from time to time, to ensure there are no very late batches of data (see 4.3 Edge case management).

For further information, type to display help message:
```shell
uv run python -m app.incremental_pipeline --help
```


### 4.3 Edge cases management
1. Late data arrival
I made a script that reproduce an incremental pipeline, with the following characteristics:
* Incremental pipeline: fast, includes a rolling reprocessing window to absorb late data
* Backfill pipeline: ensures correctness, catches very late data like 5-day late data, fixes missed updates

Hence, one can run the incremental pipeline every hours, and execute the backfill pipeline when needed (eg when data arrive late).

2. Pipeline failure
In case data pipeline fails, one should restart the pipeline given where it has failed.
Pipelines should be restartable through idempotent operations, checkpointing, and atomic writes; logging and monitoring (e.g., Grafana) helps detect failures. 
Batching also helps to isolate  and pinpoint issue. 


3. Backfill:
For this issue, one would backfill the last 30 days by  using data coming from a *source of truth*, and replaying the pipeline. This source of truth can be a data lake. 
One should consider re-processing partition by partition using an idempotent pipeline, and overwrite or merge the affected partitions in the data processed storage (eg database). One could also validate the results with data quality checks and monitor the pipeline during execution.
Here batching would help if computing ressources are limited.


### 4.4 Open Questions
For this pipeline to be used in production, I would use kafka, DBT and apache airflow as the core components. 
We assume the pipeline should be run every hour. But since there are no indications on the data volume, let's assume it is massive.
- Kafka (Depends on data volume and on the computing ressources available) : dispatches data from producers to  consumers, enables buffering, and making pipeline more scalable. 
- a data warehouse, for staging, and for analytics.
- DBT for the transformation invovling SQL queries. It also provides a way for incremental processing and for testing data integrity.
- Airflow for the orchestration layer: DAG enables scheduling and retries.
- a Grafana for monitoring metrics, logs and pipeline execution.
- AI agents (eg using Langchain): helps identify issues; useful to suggest pipeline retries or to trigger corrective actions, based on logs coming from the above tools, and metrics. It may help especially if human ressources are limited, but it should include human in the loop, as a safeguard. Note that we may remove those agents if we have heavy computing ressource limitations. 

The pipeline would look like (if computing ressources are available)
data source -> kafka -> data warehouse -> DBT <- airflow (orchestrating dbt) <-> BI + grafana + ai agents
>>>>>>> dev
