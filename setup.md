# Airflow Setup: Commands and Workflow

This guide explains the commands used to set up and run Airflow for this project: what each one does, when to run it, and what you should see. For what the pipeline itself does, see [README.md](README.md).

---

## Contents

1. [Prerequisites](#1-prerequisites)
2. [The workflow at a glance](#2-the-workflow-at-a-glance)
3. [Environment: `AIRFLOW_HOME`](#3-environment-airflow_home)
4. [Install extra packages](#4-install-extra-packages)
5. [Check the database](#5-check-the-database)
6. [Start the Airflow services](#6-start-the-airflow-services)
7. [Check the services are running](#7-check-the-services-are-running)
8. [Log in to the UI](#8-log-in-to-the-ui)
9. [After editing a DAG file](#9-after-editing-a-dag-file)
10. [Running on a different port (8081)](#10-running-on-a-different-port-8081)
11. [Stopping everything](#11-stopping-everything)
12. [Common mistakes](#12-common-mistakes)

---

## 1. Prerequisites

Before any of the commands below will work, you need:

| Requirement | How to check | Notes |
|-------------|--------------|-------|
| Linux or WSL2 | `uname -a` | This project was built on Ubuntu under WSL2 |
| Python 3.12 + `venv` | `python3 --version` | Install the venv module with `sudo apt install python3-venv` |
| Airflow installed in `~/airflow/.venv` | `~/airflow/.venv/bin/airflow version` | Should print `3.4.0`. Install steps are in [README.md → Setup](README.md#setup) |
| Virtual environment **active** | Prompt starts with `(.venv)` | `source ~/airflow/.venv/bin/activate`. Run this in **every new terminal** |
| `etl_pipeline.py` and `.env` in `~/airflow/dags/` | `ls ~/airflow/dags` | `.env` holds `DATABASE_URL` for PostgreSQL |
| A reachable PostgreSQL database | `python3 ~/airflow/dags/etl_pipeline.py` | The load step fails without it |
| `sudo` access | `sudo -v` | Only needed for installing Graphviz system packages |

---

## 2. The workflow at a glance

```
 one-time setup                      every session                        while developing
 ──────────────                      ─────────────                        ────────────────
 export AIRFLOW_HOME (in ~/.bashrc)  source .venv/bin/activate            edit a DAG file
 pip install providers-standard      mkdir -p ~/airflow/logs              airflow dags reserialize
 pip install graphviz (+ apt)        airflow db check                     airflow dags list-import-errors
                                     start api-server, scheduler,         check the UI
                                       dag-processor, triggerer
                                     pgrep / curl health
                                     cat password file → log in
```

The order matters: environment, then database check, then services, then health check, then UI.

---

## 3. Environment: `AIRFLOW_HOME`

`AIRFLOW_HOME` tells every `airflow` command where its home folder is, which holds `airflow.cfg`, the metadata database `airflow.db`, `logs/` and `dags/`.

### `echo $AIRFLOW_HOME`

Shows the current value.

```bash
echo $AIRFLOW_HOME
```

- **Empty output** means it isn't set. Airflow then falls back to `~/airflow`, which happens to be right here, but it's safer to set it explicitly.
- **`/home/<you>/airflow`** means it's set correctly.

### `export AIRFLOW_HOME=~/airflow`

Sets it **for the current terminal only**.

```bash
export AIRFLOW_HOME=~/airflow
```

- `export` makes the variable visible to programs you start from this shell, including `airflow` and the background services.
- `~` expands to your home folder (`/home/<you>`).

To make it permanent, so you don't have to repeat it in each terminal, add it to your shell profile:

```bash
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc
source ~/.bashrc
```

> If two terminals have different `AIRFLOW_HOME` values, they talk to **different** Airflow setups, each with its own database and DAG list. That's a common cause of "my DAG/run isn't in the UI".

---

## 4. Install extra packages

Run these with the venv active, so packages go into `~/airflow/.venv` and not the system Python.

### `pip install apache-airflow-providers-standard`

```bash
pip install apache-airflow-providers-standard
```

In Airflow 3, the everyday operators moved out of Airflow core into this **provider package**. This project needs it for:

| Import | Used in |
|--------|---------|
| `from airflow.providers.standard.operators.bash import BashOperator` | `users_pipeline_bash` (every task) |
| `@task.short_circuit` | Short-circuit (weekday-check) tasks |

Without it, the DAG file fails to import with `ModuleNotFoundError: No module named 'airflow.providers.standard'`. If it's already installed, pip just prints `Requirement already satisfied`, so it's safe to run again. To check:

```bash
pip show apache-airflow-providers-standard   # Name + Version (e.g. 1.21.0)
```

> When installing Airflow with a constraints file, add the same `--constraint "${CONSTRAINT_URL}"` to this command so the provider version matches your Airflow version.

### `pip install graphviz`

```bash
sudo apt update
sudo apt install graphviz     # system program that draws graphs (provides the `dot` command)
pip install graphviz          # Python library Airflow uses to call it
```

This is **optional**. Graphviz only lets the CLI draw DAG graphs as images (e.g. `airflow dags show users_pipeline_bash --save graph.png`). Without it, most `airflow dags ...` commands print this harmless warning:

```
Could not import graphviz. Rendering graph to the graphical format will not be possible.
```

You need **both** installs: the `pip` package is only a wrapper around the `apt` program. To check:

```bash
which dot               # /usr/bin/dot
pip show graphviz       # Name + Version
```

---

## 5. Check the database

### `airflow db check`

```bash
airflow db check
```

Confirms Airflow can connect to its **metadata database** (`~/airflow/airflow.db`, SQLite). That database stores DAG runs, task states, users and serialized DAGs. This is Airflow's own database, not your PostgreSQL `users` table.

Expected output:

```
[info] Connection successful.
```

If a later command asks `Please confirm database migrate`, the database hasn't been set up yet. Run `airflow db migrate` (or answer `y`).

---

## 6. Start the Airflow services

Airflow 3 is made of separate programs. Each one must be running for the system to work:

| Service | What it does | If it's not running… |
|---------|--------------|----------------------|
| `api-server` | Serves the web UI and the REST API (port 8080 by default) | No UI, and the health check `curl` fails |
| `scheduler` | Creates DAG runs on schedule and queues tasks to run | Nothing runs; triggered runs stay `queued` |
| `dag-processor` | Reads the `.py` files in `dags/` and saves each DAG to the database | New or edited DAGs never show up |
| `triggerer` | Runs *deferrable* tasks (tasks that wait without holding a worker) | Health check shows `triggerer: unhealthy`. This project's DAGs don't need it |

### Create the log folder first: `mkdir -p ~/airflow/logs`

```bash
mkdir -p ~/airflow/logs
```

- `mkdir` creates a folder, and `-p` creates parent folders too and doesn't complain if the folder already exists.
- **Run this before starting the services.** Their output is redirected into this folder, and bash won't create a missing folder for a redirect.

### Start each service in the background

```bash
nohup airflow api-server --port 8080 > ~/airflow/logs/api-server.log    2>&1 &
nohup airflow scheduler              > ~/airflow/logs/scheduler.log     2>&1 &
nohup airflow dag-processor          > ~/airflow/logs/dag-processor.log 2>&1 &
nohup airflow triggerer              > ~/airflow/logs/triggerer.log     2>&1 &
```

What each part means:

| Part | Meaning |
|------|---------|
| `nohup` | "No hang-up". The program keeps running even if you close the terminal. |
| `airflow api-server --port 8080` | Start the web server on port 8080. The other three services don't take a port. |
| `> ~/airflow/logs/<name>.log` | Send the program's normal output to that file instead of the screen. |
| `2>&1` | Send errors (stream `2`) to the same place as normal output (stream `1`), so errors land in the log file too. |
| `&` | Run in the **background** and give you the prompt back immediately. Bash prints a job number and process ID, e.g. `[1] 44476`. |

To watch a service's output live:

```bash
tail -f ~/airflow/logs/scheduler.log    # Ctrl+C to stop watching (the service keeps running)
```

> **Start each service only once.** Running these again starts *extra* copies, e.g. two schedulers, which you'll see as multiple `instances` in the health check. Stop them first (see [section 11](#11-stopping-everything)).

---

## 7. Check the services are running

### `pgrep -fl airflow`

```bash
pgrep -fl airflow
```

Lists running processes whose command line contains `airflow`.

- `pgrep` searches running processes by name.
- `-f` matches against the full command line (needed, because the program itself is `python3`).
- `-l` shows the process name next to each process ID.

Expected: one line each for `api-server`, `scheduler`, `dag-processor` and `triggerer`. The api-server may show extra worker processes. **Two** schedulers or dag-processors means you started them twice.

### `curl -s http://localhost:8080/api/v2/monitor/health`

```bash
curl -s http://localhost:8080/api/v2/monitor/health | python3 -m json.tool
```

Asks the API server for a health report.

- `curl` makes an HTTP request from the terminal, and `-s` (silent) hides the progress bar.
- `localhost:8080` is this machine on port 8080, where the api-server is listening.
- `| python3 -m json.tool` pretty-prints the JSON reply. It's optional, but much easier to read.

Healthy output (shortened):

```json
{
  "metadatabase":  { "status": "healthy" },
  "scheduler":     { "status": "healthy", "latest_scheduler_heartbeat": "..." },
  "triggerer":     { "status": "healthy", ... },
  "dag_processor": { "status": "healthy", ... }
}
```

| You see | Meaning |
|---------|---------|
| No output at all | The api-server isn't running, or it's on a different port |
| `"triggerer": {"status": "unhealthy", ... "down"}` | The triggerer isn't running. Start it |
| Several entries under `instances` | That service was started more than once |

---

## 8. Log in to the UI

### `cat ~/airflow/simple_auth_manager_passwords.json.generated`

```bash
cat ~/airflow/simple_auth_manager_passwords.json.generated
```

`cat` prints a file's contents. Airflow 3's default *Simple Auth Manager* generates an `admin` user with a random password the first time the api-server starts, and saves it in this file:

```json
{"admin": "<password>"}
```

Open **http://localhost:8080** and log in as `admin` with that password.

> Treat this file like a password: don't commit it, share it, or paste its output anywhere public.

---

## 9. After editing a DAG file

### `airflow dags reserialize`

```bash
airflow dags reserialize
```

The scheduler and UI never read your `.py` files directly. They read a saved ("serialized") copy of each DAG from the metadata database, which the dag-processor refreshes every few minutes. `reserialize` re-reads every DAG file **right now** and updates that saved copy.

- It doesn't run any DAG, delete any history, or touch your PostgreSQL data.
- Use it after editing a DAG file, when a new DAG doesn't appear, or when the UI shows an old version.

### `airflow dags list-import-errors`

```bash
airflow dags list-import-errors
```

Shows every DAG file that **failed to load**, with the Python error.

- `No data found` means every DAG file loaded. ✅
- Otherwise you get a table with the file path and a traceback. The last line of the traceback usually names the problem, e.g. `TypeError: ... missing 1 required keyword-only argument: 'task_id'`.

A file with an import error disappears from Airflow's DAG list (or stays stuck at its last good version), so **run this first whenever a DAG is missing from the UI**.

Typical edit loop:

```bash
# 1. edit etl_pipeline_dag.py
airflow dags reserialize
airflow dags list-import-errors        # want: No data found
airflow tasks list users_pipeline_bash # confirm the tasks you expect are there
```

---

## 10. Running on a different port (8081)

If port 8080 is already in use, by another program or by an api-server you already started, you can run the api-server on another port:

```bash
nohup airflow api-server --port 8081 > ~/airflow/logs/api-server.log 2>&1 &
curl -s http://localhost:8081/api/v2/monitor/health
```

Then open **http://localhost:8081** instead. Remember to use `8081` in every `curl` and browser URL.

**Stop the old services before starting new ones.** Running these again without stopping the first set gives you:

- two api-servers (on 8080 and 8081), both writing to the **same** `api-server.log`
- two schedulers and two dag-processors, visible as two `instances` in the health check

The scheduler and dag-processor don't use ports, so they never need to change. Only restart them if they've stopped.

To see what's using a port:

```bash
ss -ltnp | grep -E ':8080|:8081'
```

---

## 11. Stopping everything

```bash
pkill -f "airflow api-server"
pkill -f "airflow scheduler"
pkill -f "airflow dag-processor"
pkill -f "airflow triggerer"

pgrep -fl airflow     # should print nothing
```

`pkill -f "<text>"` stops every process whose command line contains that text. This also stops duplicate copies.

---

## 12. Common mistakes

| Command or symptom | What went wrong | Fix |
|--------------------|-----------------|-----|
| `nohup airflow triggerer > ~/airflow/run_logs/triggerer.log 2>&1 &` then `-bash: .../run_logs/triggerer.log: No such file or directory` and `Exit 1` | The `run_logs/` folder doesn't exist, so bash can't open the log file and the service never starts | Use `~/airflow/logs/` (created by `mkdir -p ~/airflow/logs`), or run `mkdir -p ~/airflow/run_logs` first |
| `irflow dags list-import-errors` → `command not found` | Typo: missing `a` | `airflow dags list-import-errors` |
| `at ~/airflow/simple_auth_manager_...` → `command not found` or an unexpected prompt | Typo: missing `c` (`at` is a different program that schedules jobs) | `cat ~/airflow/simple_auth_manager_passwords.json.generated` |
| Two commands pasted on one line, e.g. `cat file.generatedcat file.generated` | The second command was glued onto the first file name | Run one command per line |
| `airflow: command not found` | The venv isn't active in this terminal | `source ~/airflow/.venv/bin/activate` |
| `which airflow` doesn't show `.venv/bin/airflow` | A different Airflow install is being used | Activate the venv; check `echo $AIRFLOW_HOME` |
| `Could not import graphviz` warning | Graphviz isn't installed | Harmless. Install it as in [section 4](#pip-install-graphviz) to remove it |
| `ModuleNotFoundError: No module named 'airflow.providers.standard'` | The standard provider isn't installed | `pip install apache-airflow-providers-standard` |
| Health check shows two `instances` of the scheduler | Services were started twice | Stop everything ([section 11](#11-stopping-everything)), then start each once |
| UI is blank or `curl` returns nothing | The api-server isn't running, or you're using the wrong port | `pgrep -fl "airflow api-server"`; check the port in `~/airflow/logs/api-server.log` |
