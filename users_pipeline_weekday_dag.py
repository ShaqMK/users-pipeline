"""
users_pipeline_weekday

Same ETL as users_pipeline (etl_pipeline_dag.py), plus:
  1. The load is skipped on Saturday and Sunday.
  2. Raw API responses are archived to RAW_DIR; a housekeeping task gzips raw
     files older than 7 days and deletes .gz files older than 30 days. It runs
     even when the load was skipped.

The original etl_pipeline.py and etl_pipeline_dag.py are not modified.
"""
import gzip
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from etl_pipeline import extract, transform, load

# Weekday/weekend is decided in this timezone (Airflow itself runs in UTC)
LOCAL_TZ = "Africa/Nairobi"

# Raw API responses are archived here (outside dags/ so the dag-processor ignores them)
RAW_DIR = Path(os.getenv("RAW_DATA_DIR", Path.home() / "airflow" / "data" / "raw"))
COMPRESS_AFTER_DAYS = 7
DELETE_AFTER_DAYS = 30


def save_raw(raw_data):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RAW_DIR / f"users_{stamp}.json"
    path.write_text(json.dumps(raw_data, indent=2))
    print(f"[EXTRACT] Raw response saved to {path}")
    return str(path)


def housekeeping(now=None):
    """Gzip raw .json files older than COMPRESS_AFTER_DAYS and
    delete .gz files older than DELETE_AFTER_DAYS (based on file age)."""
    now = now if now is not None else time.time()
    compress_cutoff = now - COMPRESS_AFTER_DAYS * 86400
    delete_cutoff = now - DELETE_AFTER_DAYS * 86400
    compressed, deleted = [], []

    print(f"\n[HOUSEKEEPING] Scanning {RAW_DIR}")
    if not RAW_DIR.exists():
        print("[HOUSEKEEPING] Raw directory does not exist yet, nothing to do")
        return {"compressed": 0, "deleted": 0}

    for path in sorted(RAW_DIR.iterdir()):
        if not path.is_file():
            continue
        mtime = path.stat().st_mtime

        if path.suffix == ".json" and mtime < compress_cutoff:
            gz_path = path.with_name(path.name + ".gz")
            tmp_path = gz_path.with_name(gz_path.name + ".tmp")
            with path.open("rb") as src, gzip.open(tmp_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
            # keep the original age so the 30-day delete clock starts from the raw file's date
            os.utime(tmp_path, (mtime, mtime))
            tmp_path.replace(gz_path)
            path.unlink()
            compressed.append(gz_path.name)

        elif path.suffix == ".gz" and mtime < delete_cutoff:
            path.unlink()
            deleted.append(path.name)

    print(f"[HOUSEKEEPING] Compressed {len(compressed)}: {compressed}")
    print(f"[HOUSEKEEPING] Deleted {len(deleted)}: {deleted}")
    return {"compressed": len(compressed), "deleted": len(deleted)}


@dag(
    dag_id="users_pipeline_weekday",
    start_date=datetime(2026, 11, 20),
    schedule="@daily",
    catchup=False,
    tags=["etl", "lux"]
)
def users_pipeline_weekday():
    @task
    def extract_task():
        raw_data = extract()
        save_raw(raw_data)
        return raw_data

    @task
    def transform_task(raw_data):
        return transform(raw_data)

    # Returns False on Saturday/Sunday -> load_task is skipped.
    # ignore_downstream_trigger_rules=False means only the task directly after this
    # one (load_task) is skipped; tasks further down follow their own trigger_rule.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def is_weekday(**context):
        # The run's logical date (scheduled runs: the day it's for). Manual runs triggered
        # without a date have none, so fall back to when the run was triggered.
        run_date = context.get("logical_date") or context["dag_run"].run_after
        run_day = pendulum.instance(run_date).in_timezone(LOCAL_TZ)
        weekday = run_day.weekday() < 5  # Mon=0 ... Fri=4, Sat=5, Sun=6
        print(f"[CHECK] Run day is {run_day.format('dddd YYYY-MM-DD')} -> "
              f"{'loading' if weekday else 'skipping load (weekend)'}")
        return weekday

    @task
    def load_task(clean_users):
        load(clean_users)

    # none_failed: runs when load_task succeeded OR was skipped, but not if it failed
    # (so a failed load still marks the DAG run as failed).
    @task(trigger_rule="none_failed")
    def housekeeping_task():
        return housekeeping()

    # Execution order and data flow
    raw = extract_task()
    cleaned = transform_task(raw)
    loaded = load_task(cleaned)
    is_weekday() >> loaded
    loaded >> housekeeping_task()


users_pipeline_weekday()
