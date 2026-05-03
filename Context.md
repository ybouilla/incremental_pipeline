# Contexte
Pour utiliser les services Chargemap Business, les clients professionnelsdoivent posséder des licences pour leurs badges de recharge ou les bornesinstallées sur leurs parkings. Ces licences sont renouvelées chaque année.
Nous souhaitons, sur la base de données simulées, construire un pipeline dedonnées robuste permettant d'analyser l'évolution des licences dans le temps.



# Partie 1 : Génération de Données Réalistes (Python)
# Objectif
: Générer des données qui simulent un comportement métier réaliste.
Vous devez créer deux fichiers CSV :
## 1.1 Fichier initial_licenses.csv

| Colonne       | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| id            | Identifiant unique (1 à 100)                                               |
| customer_id   | Identifiant client (plusieurs licences peuvent appartenir au même client)  |
| type          | Type de licence : PASS, SIM, ou SUPERVISION                                |
| creation_date | Date de création (entre le 1er janvier 2023 et aujourd'hui)                |
| price         | Prix annuel (entre 0 et 5000)                                              |
| renewable     | Booléen indiquant si la licence est active/renouvelable                    |

## 1.2 Fichier license_changes.csv

| Colonne | Description                         |
|---------|-------------------------------------|
| id      | Identifiant unique de l'événement   |
| license_id | Référence à id dans initial_licenses.csv                          |
| date       | Date de la modification (après la date de création)               |
| price      | Nouveau prix                                                       |
| type       | Nouveau type de licence                                            |
| renewable  | Nouvel état renouvelable                                           |

Random seed: permet de rendre les résultats reproductibles.
1. Saisonnalité: Les créations de licences sont plus fréquentes en débutd'année (Q1) — période de renouvellement des contrats.
=> 
2. Distribution des clients: La plupart des clients possèdent 1 à 2 licences,mais quelques grands comptes en possèdent beaucoup plus (distributionréaliste type Pareto).
3. Cohérence des modifications:
Les augmentations de prix sont plus fréquentes que les baisses.
Les changements de type suivent une logique (ex : upgrade de PASSvers SUPERVISION plus fréquent que l'inverse).
Certaines licences ont de nombreuses modifications, la majorité en ontpeu.
4. Cohérence temporelle: Aucune modification ne peut précéder la date decréation de la licence.

Presentation du projet: 

structure : on adoptera une structure de fichier assez proche de une librairie python.

* `app`: contient les fichiers python
* `tests`: les fichiers de tests
* `utils`: les scripts permettant de generrer les données
* `config.py`: permet principalement de retrouver le root folder en un unique emplacement
* `states` permets de sauver les configs/states entre 2 executions de data pipeline 
* `logger.py`: permet de retrouver l'interface logging à un seul endroit

**package manager**

selection de uv comme package manager: un des plus rapide existant. Aussi propose une résoltion intelligente des conflits de packages. 
Uv proposes a way to install dependencies (toml.yml)

# 1. Generation des données
Hypotheses: Renewable set to True
Seulement 3 type et prix associés

prop_contract_created_Q1: prob du nombre de contrats générés en Q1.
On ne conserve que les jours de la semaine pour la création
On utilise `np.random.choice` en splittant Q1 / non Q1 (distr bernouilli)

paretto distribution des clients: values**-alpha
types: distribution en utilisant categorical distr

Modifications: varient suivant une chaine de Markov, où les 3 états / renewable/non renewable sont reepresentés

Ainsi licenses_change.md ressemble plutot à un log qu'a un une base de données (min 10 000 modifs)

# Outils pour ameliorer le code python

* linter (respecter le conventions PEP): ruff
* type checker mypy
* api building: pydantic

# validation & transformation
## validations faites sur les données respectées:
Seules les verifications proposées dans le devoir ont été réalisées

Comment améliorer: 

utiliser un outil plus adapté

- lib Daffy, Cerebrus, Pyvalidly

## transformation des données

Règles:
* Une ligne par type de licence par jour, de la plus ancienne création jusqu'à aujourd'hui
* Les jours sans modification reprennent les valeurs de la veille (différences à 0)

résultat: 

| date       | type         | active_license_count | active_license_price | inactive_license_count | daily_active_diff | daily_price_diff | daily_inactive_diff |
|------------|--------------|----------------------|----------------------|------------------------|-------------------|------------------|---------------------|
| 2023-01-11 | PASS         | 1                    | 100.0                | 0                      | 0.0               | 0                | 0.0                 |
| 2023-01-11 | SIM          | 0                    | 0                    | 0                      | 0.0               | 0                | 0.0                 |
| 2023-01-11 | SUPERVISION  | 0                    | 0                    | 0                      | 0.0               | 0                | 0.0                 |
| 2023-01-12 | PASS         | 0                    | 0                    | 0                      | -1.0              | -100.0           | 0.0                 |
| 2023-01-12 | SIM          | 0                    | 0                    | 0                      | 0.0               | 0                | 0.0                 |
| 2023-01-12 | SUPERVISION  | 1                    | 5000.0               | 0                      | 1.0               | 5000.0           | 0.0                 |


## logging, monitoring & profiling

- data profiling (= data validation): understanding the structure and quality of the data itself
tools: great expectation, pandas-profiling, dbvear
- logging: Logging is about recording events inside the pipeline execution.
tools: promotheus, Grafana loki
- monitoring: they focus more on system health, performance, and reliability than raw logs. (Grafana, Promotheus)

**Axes d'améliorations**:
- folders differents pour les logs et les csv

# Containerization
## base de données sql

Contraintes en ce qui concerne la taille de l'image docker
pour lightweight: sqlite
pour la mise en prod: postgresql

**commandes pour investiguer docker**

- **docker inspect**
- **docker history**

**axes améliorations**: 
- utilisation de clé etrangere dans la création de table
```sql
CREATE TABLE license_changes
 (  id INTEGER PRIMARY KEY,
     license_id INTEGER,
    CONSTRAINT fk_license FOREIGN KEY (license_id)
    REFERENCES initial_licenses(id),
    date DATE,
    price INTEGER,
    type VARCHAR(50),
    renewable BOOLEAN
); 
```
- requete trop large: utiliser un outil comme dbt pour split les requetes en 3 parties
- securité: eviter les attaques injections: orm, requetes correctement formulées
- massive data leakage: 
- perform a full join initial_changes * changed_licenses

## SQL query:

- utilisation de coalesce pour parer aux données manquantes.
- windows function pour calculer les requetes d'un jour à l'autre

## Tests:

utiliser `tox` ou `just` comme environement jetable pour les tests
- tester interface: playwright

**Comment intégreriez-vous ces tests dans un workflow de développement ?Décrivez brièvement ce que vous mettriez en place si vous aviez accès àGitHub Actions ou GitLab CI (pas besoin de l'implémenter, juste décrire lesétapes du pipeline CI).**

1. CI
* Define the OS environment (e.g., Ubuntu)
* Install dependencies using uv
* Run code quality checks (linting, formatting, optional type checking)
* Run and check the documentation generation
* Execute unit, integration,  consistency and end2end tests 
* Validate the full pipeline execution using Docker to ensure reproducibility
* define the workflows configuration file, ie which actions trigger when CI runs (each push request, when pushing on master branch, when releasing)

tools:
* CI orchestrator: GitHub Actions
* Dependency manager: uv
* Lint/format: Ruff
* Type checking: Mypy / pydantic
* Tests: Pytest
* environement test jetable: tox, just
* Docs: MkDocs or Sphinx
* Reproducibility: Docker
* Optional CI runner locally: act (for testing workflows)

CD : 
* run the pipeline: (kubernetes, docker, airflow, Registry: dockerhub or GitHub Container Registry)
* deploy service on cloud: AWS ECS, or railway


# Pipeline incrémental
| Aspect             | Lookback (loopback) window   | Watermark                                  |
| ------------------ | ---------------------------- | ------------------------------------------ |
| Paradigm           | Batch / incremental          | Streaming                                  |
| Goal               | Reprocess recent data        | Decide completeness of data                |
| Late data handling | Re-read past data            | Accept until threshold, then drop/redirect |
| Cost               | Extra compute (reprocessing) | More state & coordination                  |
| Complexity         | Simple                       | More advanced                              |


## 1. Conception

Décrivez votre approche pour un pipeline incrémental :
1. Comment suivez-vous ce qui a déjà été traité ? (watermark, checkpoint,curseur...)
 * high water mark, update_at new variable
 * mise à jour des states
2. Comment mettez-vous à jour les agrégations existantes sans tout recalculer?
* lookback window : on effectue les traitements depuis la date last_processed_timestamp - window: plutot en mode incremental
* watermark (delay avant de process les données), plutot utilise en streaming: decide à quel moment on considere les données comme late
3. Quel état persistez-vous entre les exécutions ?
les états peuvent etre conservés dans une database :
 - idempotent: utilisation des transactions
 - utilisation d'une  primary key, pour verifier qu'il n'y ait pas de doublons

 ```sql
 IF EXISTS (SELECT id FROM table WHERE id = 1)
    UPDATE table SET col1 = 'a', col2 = 'b' WHERE id = 1;
ELSE
    INSERT INTO table (id, col1, col2) VALUES (1, 'a', 'b');
 ```
 - autre possibilité: utiliser airflow pour le backfill.

 ## 2. Implémentation: `incremental_pipeline.py`

these pipeline are assuming transforming data from datalake to a data warehouse

 - run_backfile_pipeline: for late arrival data, with a specific definition of windows: window = now - 5 days


 - run_pipeline: parse and store data into a data warehouse
 definition of window = last_processed_timestamp - loopback_windows (30 min)
    This pipeline:
    1. Loads checkpoint state (watermarks)
    2. Defines processing window
    3. Fetches source data
    4. Loads target datasets
    5. Processes data in batches with retries
    6. Updates watermarks
    7. Persists final checkpoint

## 4.3 cas limites

1. Données tardives
Une modification datée du 15 janvier arrive le 20 janvier. Commentrecalculer les agrégations impactées ?

* creation de plusieurs pipelines: en fonction du retard des données.
* utilisation d'un datalake (apache iceberg) par partion et d'un batch processing pour éviter que le reprocessing ne soit trop lourd au niveau du calcul
* utilisation du script run_backfile_pipeline
* idempotent

2. Reprise sur erreur
Le pipeline échoue à mi-parcours. Comment garantir une reprisepropre sans corruption de données ?
la pipeline doit garantir a minima:
- idempotence (pas de changement si la pipeline est executée plusieurs fois)
- operations write atomiques (ou staging): les operations write sont successfuls.
- checkpointing/ state tracking (date du dernier succes de la pipeline)
Utilisation de airflow pour relancer la pipeline, et Grafana pour monitoring de la pipeline. 

3. Backfill
On vous demande de recalculer les 30 derniers jours suite à unecorrection de bug. Comment procédez-vous ?

 * Utilisation de partitions / daily batches depuis un datalake.
Process des partitions depuis un datalake; 
```bash
airflow dags backfill my_pipeline -s 2026-04-03 -e 2026-05-03
```
* sql: utilisation de upsert: 
```sql
INSERT INTO my_table (id, col1, col2)
VALUES (1, 'a', 'b')
ON CONFLICT (id)
DO UPDATE SET
    col1 = excluded.col1,
    col2 = excluded.col2;
```
* ETL job → staging_table → production_table

## 4.4 questions ouvertes
note sur kafka:
Kafka permt de rendre la solution plus scalable, cad :
- buffering (absorbs spikes in data volume)
- replay (reprocess past data)
- fault tolerance (systeme continue a operer meme en cas de failure d'un des components)

### a. non streaming, huge chunk hourly
- ingestion (airbytes) / spark apache  batch processing mode
- data lake, comme iceberg
- Trino
- DBT for transformation (mode incremental)
- airflow for ochestration
- grafana for monitring


### b. streaming/micro batch
- kafka 
- spark streaming mode (micro batch) or flink (true real time)
- Iceberg
- trino / spark batch mode
- airflow for ochestration
- grafana for monitring (prometheus/loki)

### c. non streaming, but large varibility in the volume  and pipeline run often (near-real time processing; 1 min à 10 min)
- kafka (for buffering)
- postgresql
- DBT for transformation 
- airflow for orchestration
- grafana for monitring




**Batch** (hours/daily): Airbyte → Databricks → dbt → Airflow
**Near real-time (minutes)**:  maybe Kafka if buffering is needed -> Airbyte → Databricks → dbt → Airflow
**Streaming (seconds)**: Kafka → Spark/Databricks streaming → lakehouse
**True streaming complexity** = only when latency is critical, not just “high volume”


atomic writes: comme sql transactions, all or nothing operations

### data quality
**Bronze (raw)**
append-only
no transformations
acts as safety net
**Silver (cleaned)**
deduplicated
structured
validated
**Gold (aggregated)**
business-ready metrics

technos à maitriser:
- kafka
- airflow
- Trino
- kubernetes
- apache iceberg (datalake)
- datawarhouse: databricks sql, postgresql, duckdb
- grafana & prometheus