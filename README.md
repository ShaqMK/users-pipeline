# Users ETL Pipeline with Apache Airflow

A small ETL (Extract, Transform, Load) pipeline. It pulls user records from a public REST API, cleans and validates them, and upserts them into a PostgreSQL table. Apache Airflow 3 orchestrates it on a daily schedule.

The pipeline was first written as a standalone script in `~/data-engineering-lux/etl-s01/`. It was then copied into this Airflow `dags/` folder and wrapped in a DAG.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Setup](#setup)
5. [Running the Pipeline](#running-the-pipeline)
6. [Verifying the Setup](#verifying-the-setup)
7. [Stopping Airflow](#stopping-airflow)
8. [Troubleshooting](#troubleshooting)

---

## How It Works

```
┌──────────────┐     ┌────────────────┐     ┌──────────────┐
│ extract_task │ ──▶ │ transform_task │ ──▶ │  load_task   │
└──────────────┘     └────────────────┘     └──────────────┘
  GET JSON API         clean + validate       upsert into
                                              Postgres "users"
```

| Stage | Function | What it does |
|-------|----------|--------------|
| **Extract** | `extract()` | Sends a `GET` request to `https://jsonplaceholder.typicode.com/users` with a 10 s timeout. It fails on a non-2xx status or an empty response and returns the raw list of users. |
| **Transform** | `transform(raw_data)` | Flattens each user into a record. It collapses extra whitespace (`clean_text`), strips phone numbers down to digits and drops extensions (`clean_phone`), defaults a missing city to `"unknown"` and lowercases emails. Records with no id or name, or with an invalid email, are skipped. |
| **Load** | `load(clean_users)` | Connects to PostgreSQL using `DATABASE_URL`. It creates the `users` table if it doesn't exist, then **upserts** every record (`INSERT ... ON CONFLICT (user_id) DO UPDATE`), so re-runs are idempotent. |

The three stages pass data to each other through Airflow **XComs**, using the TaskFlow API (`@dag` / `@task`).

### DAG details

| Property | Value |
|----------|-------|
| `dag_id` | `users_pipeline` |
| Schedule | `@daily` |
| Start date | `2026-11-20` |
| Catchup | `False` (missed past runs are not backfilled) |
| Tags | `etl`, `lux` |

### Target table

```sql
CREATE TABLE IF NOT EXISTS users (
    user_id    INTEGER PRIMARY KEY,
    full_name  TEXT NOT NULL,
    username   TEXT,
    email      TEXT NOT NULL,
    phone      TEXT,
    city       TEXT,
    website    TEXT,
    loaded_at  TIMESTAMP DEFAULT NOW()
);
```

---

## Project Structure

```
~/airflow/                      # AIRFLOW_HOME
├── .venv/                      # Python virtual environment (Airflow + deps)
├── airflow.cfg                 # Airflow configuration (auto-generated)
├── airflow.db                  # SQLite metadata database (auto-generated)
├── simple_auth_manager_passwords.json.generated   # UI login (auto-generated)
├── logs/                       # Task logs + service logs
└── dags/                       # ← this repository
    ├── etl_pipeline.py         # extract / transform / load functions
    ├── etl_pipeline_dag.py     # Airflow DAG definition (users_pipeline)
    ├── hello_dag.py            # placeholder (empty)
    ├── .env                    # DATABASE_URL (not committed)
    └── .gitignore              # ignores .env
```

---

## Prerequisites

- **Linux / WSL2** (this project was built on Ubuntu under WSL2)
- **Python 3.12** with the `venv` module (`sudo apt install python3-venv` if it's missing)
- A reachable **PostgreSQL** database, local or hosted, and its connection string
- Internet access, to reach the source API
- *(Optional)* **Graphviz**, so DAG graphs can be rendered from the CLI

---

## Setup

### 1. Create the Airflow home and DAGs folder

```bash
mkdir -p ~/airflow/dags ~/airflow/logs
cd ~/airflow
```

### 2. Set `AIRFLOW_HOME`

Airflow uses this directory for its config, metadata database and logs. To make the setting permanent, add it to your shell profile:

```bash
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc
source ~/.bashrc
echo $AIRFLOW_HOME     # → /home/<you>/airflow
```

> Airflow falls back to `~/airflow` when this variable is unset. Setting it explicitly prevents surprises when you run commands from other directories.

### 3. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
```

Run `source ~/airflow/.venv/bin/activate` again in every new terminal before using `airflow`. To confirm the right binary is in use:

```bash
which airflow    # → /home/<you>/airflow/.venv/bin/airflow
```

### 4. Install Apache Airflow

Install Airflow against the official constraints file that matches your Python version, so its dependency versions stay compatible:

```bash
AIRFLOW_VERSION=3.4.0
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
airflow version   # → 3.4.0
```

### 5. Install the pipeline dependencies

```bash
pip install requests psycopg2-binary python-dotenv
```

These match `requirements.txt` in the original `etl-s01` project.

### 6. Add the DAG files

Place `etl_pipeline.py` and `etl_pipeline_dag.py` in `~/airflow/dags/`. Clone this repository into that folder, or copy the files from the original project:

```bash
cp ~/data-engineering-lux/etl-s01/etl_pipeline.py ~/airflow/dags/
```

### 7. Configure the database connection

Create `~/airflow/dags/.env` with your PostgreSQL connection string. You can copy it from the original project:

```bash
cp ~/data-engineering-lux/etl-s01/.env ~/airflow/dags/.env
```

Or write it by hand:

```dotenv
DATABASE_URL=postgresql://<user>:<password>@<host>:<port>/<database>
```

> `.env` is listed in `.gitignore`. **Never commit it.** `load_dotenv()` in `etl_pipeline.py` finds it by searching from the script's own directory.

### 8. Test the pipeline outside Airflow

Before scheduling the pipeline, check that it works end to end:

```bash
cd ~/airflow/dags
python3 etl_pipeline.py
```

You should see the `[EXTRACT]`, `[TRANSFORM]` and `[LOAD]` log lines, and the `users` table should be populated.

### 9. Initialise the metadata database

```bash
airflow db migrate
airflow db check        # → "Connection successful."
```

> If you skip this step, the first `airflow dags ...` command will ask you to confirm a database migration. Answer `y`.

### 10. *(Optional)* Install Graphviz

This removes the `Could not import graphviz` warning from CLI commands:

```bash
sudo apt update
sudo apt install graphviz
pip install graphviz
```

---

## Running the Pipeline

Airflow 3 runs as separate services. Start each one in the background and send its output to `~/airflow/logs/`. Make sure that directory exists first. A redirect into a folder that doesn't exist, such as `run_logs/`, makes the command exit immediately with `No such file or directory`.

```bash
source ~/airflow/.venv/bin/activate
export AIRFLOW_HOME=~/airflow
mkdir -p ~/airflow/logs

nohup airflow api-server --port 8080 > ~/airflow/logs/api-server.log    2>&1 &
nohup airflow scheduler              > ~/airflow/logs/scheduler.log     2>&1 &
nohup airflow dag-processor          > ~/airflow/logs/dag-processor.log 2>&1 &
nohup airflow triggerer              > ~/airflow/logs/triggerer.log     2>&1 &
```

| Service | Role |
|---------|------|
| `api-server` | Serves the web UI and REST API |
| `scheduler` | Decides when DAG runs and tasks should execute |
| `dag-processor` | Parses files in `dags/` and serialises DAGs into the metadata DB |
| `triggerer` | Runs deferrable tasks. This DAG doesn't need it, but without it the health check reports `unhealthy` |

> **Start each service only once.** Running the start commands a second time launches duplicate schedulers and DAG processors, which shows up as multiple `instances` in the health check. If port 8080 is taken, use another port (e.g. `--port 8081`), but stop the old services first (see [Stopping Airflow](#stopping-airflow)).

### Log in to the UI

Open **http://localhost:8080**. The default *Simple Auth Manager* creates an `admin` user with a random password. To read it:

```bash
cat ~/airflow/simple_auth_manager_passwords.json.generated
```

### Trigger the DAG

New DAGs start **paused**. Unpause and trigger the DAG from the UI (search for `users_pipeline`), or from the CLI:

```bash
airflow dags unpause users_pipeline
airflow dags trigger users_pipeline
```

After that it runs automatically once a day.

---

## Verifying the Setup

```bash
# Is the metadata DB reachable?
airflow db check

# Did any DAG file fail to import? (should print "No data found")
airflow dags list-import-errors

# Is users_pipeline registered?
airflow dags list | grep users_pipeline

# Force DAGs to be re-parsed after editing files
airflow dags reserialize

# Which Airflow processes are running?
pgrep -fl airflow

# Service health (metadatabase, scheduler, triggerer, dag_processor)
curl -s http://localhost:8080/api/v2/monitor/health | python3 -m json.tool
```

A healthy setup reports `"status": "healthy"` for every component.

To confirm the data was loaded, check PostgreSQL:

```sql
SELECT user_id, full_name, email, phone, city, loaded_at FROM users ORDER BY user_id;
```

---

## Stopping Airflow

```bash
pkill -f "airflow api-server"
pkill -f "airflow scheduler"
pkill -f "airflow dag-processor"
pkill -f "airflow triggerer"

pgrep -fl airflow     # should print nothing
```

---

## Troubleshooting

| Symptom | Cause / Fix |
|---------|-------------|
| `-bash: .../run_logs/xxx.log: No such file or directory` and the job exits with `Exit 1` | The log directory doesn't exist. Run `mkdir -p ~/airflow/logs` and redirect output there. |
| `Could not import graphviz` warning | Harmless. Install Graphviz (Setup step 10) to remove it. |
| `Please confirm database migrate` prompt | The metadata DB hasn't been initialised. Run `airflow db migrate`. |
| `triggerer` shows `unhealthy` / `down` | The triggerer isn't running. Start it with `nohup airflow triggerer ...`. |
| Multiple scheduler / dag-processor `instances` in health output | The services were started more than once. Stop all of them and start each one once. |
| `users_pipeline` doesn't appear in the UI | Run `airflow dags list-import-errors`. Check that the `dag-processor` is running and that `AIRFLOW_HOME` points to `~/airflow`. |
| `ModuleNotFoundError: etl_pipeline` | `etl_pipeline.py` must be in the same `dags/` folder as `etl_pipeline_dag.py`. |
| `load_task` fails with a connection error | Check `DATABASE_URL` in `dags/.env` and confirm PostgreSQL is reachable. Running `python3 etl_pipeline.py` reproduces the error outside Airflow. |
| Many example DAGs (e.g. `aggregate_regional_sales`) in the list | These are Airflow's bundled examples. To hide them, set `load_examples = False` in `airflow.cfg` and restart the services. |
| `airflow: command not found` | The virtual environment isn't active. Run `source ~/airflow/.venv/bin/activate`. |
