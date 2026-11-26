# Users ETL Pipeline with Apache Airflow

A small ETL (Extract, Transform, Load) pipeline. It pulls user records from a public REST API, cleans and validates them, and upserts them into a PostgreSQL table. Apache Airflow 3 orchestrates it on a daily schedule.

The pipeline was first written as a standalone script in `~/data-engineering-lux/etl-s01/`. It was then copied into this Airflow `dags/` folder and wrapped in a DAG.

`etl_pipeline_dag.py` defines two DAGs that run the same ETL in different ways:

| DAG | Built with | What it adds | Default state |
|-----|------------|--------------|---------------|
| `users_pipeline` | Python `@task` functions (TaskFlow API) | Nothing; plain extract → transform → load | Paused |
| `users_pipeline_bash` | `BashOperator` shell commands | Checks the project files, skips the load on weekends, cleans up old raw files | **Active** |

Only one of them should be unpaused at a time. Both load into the same `users` table.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [The Bash DAG: users_pipeline_bash](#the-bash-dag-users_pipeline_bash)
3. [Jinja Templates](#jinja-templates)
4. [Project Structure](#project-structure)
5. [Prerequisites](#prerequisites)
6. [Setup](#setup)
7. [Running the Pipeline](#running-the-pipeline)
8. [Verifying the Setup](#verifying-the-setup)
9. [Stopping Airflow](#stopping-airflow)
10. [Troubleshooting](#troubleshooting)

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

In `users_pipeline`, the three stages pass data to each other through Airflow **XComs**, using the TaskFlow API (`@dag` / `@task`). `users_pipeline_bash` runs all three stages in one task instead (see below).

### DAG details: users_pipeline

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

## The Bash DAG: users_pipeline_bash

This DAG runs the same ETL, but each step is a **shell command**, run by Airflow's `BashOperator` the same way you'd type it in a terminal. It also adds a weekend check and a cleanup step.

```
┌─────────────┐   ┌───────────────┐   ┌──────────────┐   ┌─────────────┐   ┌──────────┐
│ check_files │──▶│ weekdays_only │──▶│ run_pipeline │──▶│ last_update │──▶│ clean_up │
└─────────────┘   └───────────────┘   └──────────────┘   └─────────────┘   └──────────┘
 project files      Sat/Sun → skip      python               print run         gzip / delete
 present?           the rest            etl_pipeline.py      date              old raw files
```

| Property | Value |
|----------|-------|
| `dag_id` | `users_pipeline_bash` |
| Schedule | `@daily` (00:00 UTC = 03:00 Nairobi time) |
| Start date | `2026-11-20` |
| Catchup | `False` |
| Tags | `etl`, `lux`, `bash` |

### What happens on each day

| Day | `check_files` | `weekdays_only` | `run_pipeline` | `last_update` | `clean_up` | Run result |
|-----|---------------|-----------------|----------------|---------------|------------|------------|
| Mon–Fri | ✅ success | ✅ success | ✅ success | ✅ success | ✅ success | success |
| Sat–Sun | ✅ success | ⏭ skipped | ⏭ skipped | ⏭ skipped | ✅ success | success |

On weekends `weekdays_only` itself appears **skipped** (pink) in the UI. That's expected, not an error (see [exit code 99](#how-the-weekend-skip-works)).

### Variables used by the commands

```python
PROJ_DIR = "$HOME/airflow/dags"
RAW_DIR  = "$HOME/airflow/data/raw"
RUN_DATE = "(dag_run.logical_date or dag_run.run_after)"
```

- **`PROJ_DIR` / `RAW_DIR`** contain `$HOME`, which **bash** expands to your home folder (`/home/<you>`) when the command runs. Python leaves it as plain text.
- **`RAW_DIR`** is outside `dags/` on purpose. The dag-processor scans everything in `dags/`, so data files shouldn't live there.
- **`RUN_DATE`** is a Jinja expression (see [Jinja Templates](#jinja-templates)) for the date the run is *for*. Scheduled runs have a `logical_date`. Runs triggered manually from the UI or CLI usually don't, so it falls back to `run_after`, the time the run was triggered. It must be written as `dag_run.logical_date`: when a run has no logical date, Airflow leaves the bare `logical_date` variable out entirely, and the template fails with `'logical_date' is undefined`.

### Task by task

#### 1. `check_files`: are the project files where we expect them?

```bash
cd $HOME/airflow/dags && ls etl_pipeline.py .env && echo 'Project files found'
```

| Part | Meaning |
|------|---------|
| `cd $HOME/airflow/dags` | Move into the DAGs folder. |
| `&&` | "Only run the next command if the previous one succeeded." If `cd` fails, nothing after it runs and the task fails. |
| `ls etl_pipeline.py .env` | List those two files. `ls` exits with an error if either one is missing, which fails the task. |
| `echo 'Project files found'` | Print a confirmation to the task log. It only runs if both files exist. |

This catches a missing script or `.env` *before* anything else runs.

#### 2. `weekdays_only`: skip the rest on Saturday and Sunday

What's in the DAG file:

```bash
DAY={{ (dag_run.logical_date or dag_run.run_after).strftime('%u') }};
echo "Day of the week: $DAY";
if [ "$DAY" -ge 6 ]; then echo 'weekend: skipping'; exit 99; fi
```

What bash actually receives after Airflow fills in the template, e.g. on a Wednesday:

```bash
DAY=3;echo "Day of the week: $DAY";if [ "$DAY" -ge 6 ]; then echo 'weekend: skipping'; exit 99; fi
```

| Part | Meaning |
|------|---------|
| `{{ ... .strftime('%u') }}` | Airflow replaces this with the run date's **ISO weekday number**: `1` = Monday … `5` = Friday, `6` = Saturday, `7` = Sunday. |
| `DAY=3` | Create a shell variable `DAY`. There must be **no spaces** around `=` in bash. |
| `;` | Command separator: run the next command whether or not the previous one succeeded. |
| `echo "Day of the week: $DAY"` | Print the value. `$DAY` reads the variable. Double quotes let bash expand it. |
| `if [ ... ]; then ...; fi` | A bash *if* block. `[ ... ]` is the test, and `fi` ("if" backwards) closes the block. |
| `"$DAY" -ge 6` | "Is `DAY` **g**reater than or **e**qual to 6?", i.e. Saturday or Sunday. `-ge` is for numbers; `>=` doesn't work inside `[ ]`. |
| `exit 99` | Stop the script with exit code 99. |

##### How the weekend skip works

Every shell command ends with an **exit code**. BashOperator turns it into a task state:

| Exit code | Task state |
|-----------|------------|
| `0` | success |
| `99` | **skipped** (BashOperator's default `skip_on_exit_code`) |
| anything else | failed |

When `weekdays_only` is skipped, the tasks after it that use the default trigger rule (`all_success`) are skipped too. That covers `run_pipeline` and `last_update`, so **nothing is loaded on weekends**. The check runs *before* `run_pipeline` on purpose: if it ran afterwards, the load would already have happened.

#### 3. `run_pipeline`: run the ETL script

```bash
cd $HOME/airflow/dags && python etl_pipeline.py
```

| Part | Meaning |
|------|---------|
| `cd $HOME/airflow/dags` | Move into the folder that has `etl_pipeline.py` and `.env`. |
| `&& python etl_pipeline.py` | Run the whole extract → transform → load script, exactly like running it in a terminal. If the script raises an error, it exits with a non-zero code and the task fails. |

`python` is found through the `PATH` of the Airflow process. Because the scheduler was started inside `~/airflow/.venv`, it's the venv's Python, which has `requests`, `psycopg2` and `python-dotenv` installed. Everything the script prints (`[EXTRACT] ...`, `[LOAD] 10 rows ...`) shows up in the task log.

#### 4. `last_update`: log the run date

```bash
echo 'Pipeline last update for {{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}'
```

Airflow fills in the date, so bash runs e.g. `echo 'Pipeline last update for 2026-11-25'`. `strftime('%Y-%m-%d')` formats the date as year-month-day.

#### 5. `clean_up`: compress and delete old raw files

```bash
mkdir -p $HOME/airflow/data/raw && cd $HOME/airflow/data/raw || exit 1;
find . -name '*.json' -mtime +7 -exec gzip -f {} + ;
find . -name '*.json.gz' -mtime +30 -delete ;
echo "Files Kept: $(ls | wc -l)"
```

**Line 1: go to the raw folder, or stop**

| Part | Meaning |
|------|---------|
| `mkdir -p <dir>` | Create the folder, including any missing parent folders. `-p` also means "no error if it already exists". |
| `&& cd <dir>` | Then move into it. |
| `\|\| exit 1` | `\|\|` means "only run the next command if the previous one **failed**". If the folder can't be created or entered, stop with code 1 (task fails). This is a safety guard: without it, the `find ... -delete` below would run in whatever folder the task happened to start in. |

**Line 2: gzip raw files older than 7 days**

| Part | Meaning |
|------|---------|
| `find .` | Search the current folder (`.`) and its subfolders. |
| `-name '*.json'` | Only files whose name ends in `.json`. The single quotes stop bash from expanding `*` itself, so `find` gets the pattern. |
| `-mtime +7` | Last **m**odified more than 7 full days ago. `find` counts in whole 24-hour periods and ignores fractions, so `+7` matches files at least 8 days old. |
| `-exec gzip -f {} +` | Run `gzip` on the matches. `{}` is replaced with the file names, and `+` passes many files to one `gzip` call. `gzip` replaces `file.json` with `file.json.gz` and **keeps the original modification time**. `-f` (force) overwrites a `.gz` with the same name if one exists. |

**Line 3: delete compressed files older than 30 days**

| Part | Meaning |
|------|---------|
| `-name '*.json.gz'` | Only compressed files. |
| `-mtime +30` | Older than 30 full days. Because `gzip` kept the original timestamp, this counts from when the raw file was saved, not when it was compressed. |
| `-delete` | Delete the matches. There's no confirmation or recycle bin, which is why line 1 has the `\|\| exit 1` guard. |

**Line 4: report**

| Part | Meaning |
|------|---------|
| `$(ls \| wc -l)` | *Command substitution*: run `ls \| wc -l` and insert its output. `ls` lists the files, and `\|` (pipe) sends that list to `wc -l`, which counts the lines, i.e. the number of files left. |

**`trigger_rule="none_failed"`**: by default a task only runs if everything before it **succeeded**. `none_failed` means "run as long as nothing before me **failed**", so skipped is fine. That's why `clean_up` still runs on weekends. If the pipeline actually fails, `clean_up` doesn't run, and the DAG run is still marked failed.

> **Note:** `etl_pipeline.py` doesn't save raw API responses to disk. `clean_up` only has work to do when something writes `*.json` files into `~/airflow/data/raw/`. Until then it just reports how many files are there.

The `{{ ... }}` parts in `weekdays_only` and `last_update` are Jinja templates. See [Jinja Templates](#jinja-templates) for how they work.

### Bash quick reference

| Symbol | Meaning | Example from this DAG |
|--------|---------|-----------------------|
| `&&` | Run next only if previous **succeeded** | `cd ... && ls ...` |
| `\|\|` | Run next only if previous **failed** | `cd ... \|\| exit 1` |
| `;` | Run next regardless | `DAY=3; echo ...` |
| `\|` | Pipe output of one command into another | `ls \| wc -l` |
| `$VAR` | Read a variable | `$DAY`, `$HOME` |
| `$( ... )` | Insert a command's output | `$(ls \| wc -l)` |
| `'...'` | Literal text, no expansion | `'*.json'` |
| `"..."` | Text with `$VAR` expanded | `"Day of the week: $DAY"` |
| `exit N` | End with exit code N (0 = success) | `exit 99`, `exit 1` |

---

## Jinja Templates

**Jinja** is a templating language for Python. You write text with placeholders in it, and Jinja fills those placeholders in later. Airflow uses it so that **one task definition can adapt to each run**. The same command gets a different date, run ID and so on each time it runs.

### How Airflow uses it

This is what's written in the DAG file for `last_update`:

```python
bash_command="echo 'Pipeline last update for {{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}'"
```

Each time the task runs, Airflow:

1. Takes that string.
2. Works out everything inside `{{ }}` using that run's details.
3. Hands the finished command to bash, e.g. `echo 'Pipeline last update for 2026-11-25'`.

Tomorrow's run gets tomorrow's date, and the code doesn't change.

### Syntax

| Syntax | Purpose | Example |
|--------|---------|---------|
| `{{ ... }}` | **Output** a value | `{{ dag_run.run_id }}` |
| `{% ... %}` | **Logic**: if/else, loops | `{% if dag_run.run_type == 'manual' %}echo manual{% endif %}` |
| `{# ... #}` | Comment, removed from the output | `{# why this exists #}` |
| `\|` | **Filter**, which transforms a value | `{{ dag.dag_id \| upper }}` gives `USERS_PIPELINE_BASH` |

### What you can use inside `{{ }}`

**Template variables** are details about the current run:

| Variable | What it is | Example value |
|----------|------------|---------------|
| `dag_run` | The current DAG run | `dag_run.run_id` gives `manual__2026-11-25T10:49:27…` |
| `dag_run.logical_date` | The date the run is *for*. `None` for manual runs triggered without a date | `2026-11-25 00:00:00+00:00` |
| `dag_run.run_after` | When the run was scheduled or triggered. Always set | `2026-11-25 10:49:27+00:00` |
| `logical_date`, `ds` | Shortcuts for the logical date (`ds` = `YYYY-MM-DD` string). **Missing entirely** when the run has no logical date | `2026-11-25` |
| `dag`, `task`, `ti` | The DAG, the task, and the task instance | `{{ task.task_id }}` gives `last_update` |
| `params` | Values passed in when the DAG is triggered | `{{ params.region }}` |

**Macros** are helper functions that work on those values:

| Macro | What it does | Example | Result |
|-------|--------------|---------|--------|
| `macros.ds_add(ds, n)` | Add or subtract days from a `YYYY-MM-DD` string | `{{ macros.ds_add(ds, -7) }}` | `2026-11-18` |
| `macros.ds_format(ds, in, out)` | Convert a date string from one format to another | `{{ macros.ds_format(ds, '%Y-%m-%d', '%u') }}` | `3` (Wednesday) |
| `macros.datetime` / `macros.timedelta` | Python's `datetime` and `timedelta` | `{{ (dag_run.run_after - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}` | the day before the run |
| `macros.uuid`, `macros.random`, `macros.time` | Python's `uuid`, `random` and `time` modules | `{{ macros.uuid.uuid4() }}` | a unique ID |

**Ordinary Python on those values** also works, such as method calls and `or`, as in `.strftime('%u')` and `logical_date or run_after`.

### Why this DAG uses `dag_run.logical_date or dag_run.run_after`

Runs triggered manually from the UI or CLI usually have **no logical date**. In that case Airflow leaves `ds` and `logical_date` out of the template completely. Any template that uses them fails before the command even runs:

```
UndefinedError: 'logical_date' is undefined
```

`dag_run.logical_date` is always there (it's just `None` when there's no date), so `dag_run.logical_date or dag_run.run_after` works for both scheduled and manual runs. The DAG stores this expression once as `RUN_DATE` and reuses it.

Common date formats used with `.strftime()`:

| Code | Meaning | Example |
|------|---------|---------|
| `%Y-%m-%d` | Year-month-day | `2026-11-25` |
| `%u` | ISO weekday, 1 = Monday … 7 = Sunday | `3` |
| `%A` | Weekday name | `Wednesday` |
| `%H:%M` | Hour:minute (24-hour) | `10:49` |

### Where templates work

Only in an operator's **templated fields**. For `BashOperator`, those are `bash_command`, `env` and `cwd`. If you put `{{ ds }}` in a field that isn't templated, such as `task_id`, it stays as that literal text. Each operator's docs list its `template_fields`.

Templates also don't work in plain Python code. Inside a `@task` function, read the same values from the task context instead:

```python
from airflow.sdk import get_current_context

@task
def my_task():
    run_date = get_current_context()["dag_run"].run_after
```

### Jinja vs Python f-strings

| | f-string `f"...{x}..."` | Jinja `"...{{ x }}..."` |
|-|-------------------------|-------------------------|
| Filled in by | Python | Airflow |
| When | Once, when the dag-processor parses the file | Every time the task runs |
| Knows about the run? | No | Yes: dates, run ID and so on |
| Used in this DAG for | `PROJ_DIR`, `RAW_DIR` (fixed paths) | The run date in `weekdays_only` and `last_update` |

Don't put Jinja inside an f-string. In an f-string, `{{` turns into a single `{`, so `f"{{ ds }}"` reaches Airflow as `{ ds }`, which isn't a template. That's why the templated commands are built by joining plain strings with `+`:

```python
"DAY={{ " + RUN_DATE + ".strftime('%u') }};"
```

### Seeing the filled-in result

- **In the UI:** open a task instance and go to the **Rendered Template** tab. It shows the final command after Jinja ran.
- **In the task log:** look for the line `Running command: ['/usr/bin/bash', '-c', 'DAY=3;...']`.
- **From the CLI:** `airflow tasks render users_pipeline_bash weekdays_only 2026-11-25` prints the filled-in fields for that date without running anything.

---

## Project Structure

```
~/airflow/                      # AIRFLOW_HOME
├── .venv/                      # Python virtual environment (Airflow + deps)
├── airflow.cfg                 # Airflow configuration (auto-generated)
├── airflow.db                  # SQLite metadata database (auto-generated)
├── simple_auth_manager_passwords.json.generated   # UI login (auto-generated)
├── logs/                       # Task logs + service logs
├── data/raw/                   # Raw files cleaned up by users_pipeline_bash (created on first run)
└── dags/                       # ← this repository
    ├── etl_pipeline.py         # extract / transform / load functions
    ├── etl_pipeline_dag.py     # both DAGs: users_pipeline and users_pipeline_bash
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

> For a command-by-command explanation of starting, checking and stopping Airflow (what each command and flag means, and the common mistakes), see [setup.md](setup.md).

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

New DAGs start **paused**. Unpause **one** of the two DAGs. To use the bash version (recommended):

```bash
airflow dags pause users_pipeline          # make sure the Python version is off
airflow dags unpause users_pipeline_bash
airflow dags trigger users_pipeline_bash   # run it now instead of waiting for 00:00 UTC
```

Or do the same from the UI: search for `users_pipeline_bash` and use the pause toggle and the **Trigger** button. After that it runs automatically once a day.

> A run triggered on a **paused** DAG stays `queued` and never starts. Unpause it first. To test a DAG without unpausing it, run `airflow dags test <dag_id> <YYYY-MM-DD>`, which runs every task right away for that date. A Saturday date is a quick way to check the weekend skip.

To see what each command printed, open the DAG in the UI, click a task in the **Graph** or **Grid** view, then open **Logs**.

---

## Verifying the Setup

```bash
# Is the metadata DB reachable?
airflow db check

# Did any DAG file fail to import? (should print "No data found")
airflow dags list-import-errors

# Are both DAGs registered? (and which one is paused)
airflow dags list | grep users_pipeline

# Tasks in the bash DAG (should list 5)
airflow tasks list users_pipeline_bash

# Recent runs and their states
airflow dags list-runs users_pipeline_bash

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
| UI shows only 3 tasks | You're looking at `users_pipeline`. The bash version is `users_pipeline_bash`, which has 5 tasks. Both are defined in the same file. |
| Triggered run stays `queued` | The DAG is paused. Unpause it, or test it with `airflow dags test`. |
| `'logical_date' is undefined` in a task log | A template uses the bare `logical_date` variable, which is missing for runs triggered without a date. Use `dag_run.logical_date` instead (as `RUN_DATE` does). |
| `weekdays_only` is pink/skipped | Expected on Saturday and Sunday: the command exited with code 99. Check the log for `Day of the week: 6` or `7`. |
| `TypeError: ... missing 1 required keyword-only argument: 'task_id'` | A typo in an operator's `task_id=` argument (e.g. `task_is=`). Every operator needs a `task_id`. |
| `ls: cannot access ...` in `check_files` | `etl_pipeline.py` or `.env` is missing from `~/airflow/dags/`. |
| The data is loaded twice a day | Both `users_pipeline` and `users_pipeline_bash` are unpaused. Pause one. |
