from datetime import datetime
from airflow.sdk import dag, task
from etl_pipeline import extract, transform, load
from airflow.providers.standard.operators.bash import BashOperator

PROJ_DIR = "$HOME/airflow/dags"


@dag(
    dag_id="users_pipeline",
    start_date=datetime(2026, 11, 20),
    schedule="@daily",
    catchup=False,
    tags=["etl", "lux"]
)


def users_pipeline():
    @task
    def extract_task():
        return extract()

    @task
    def transform_task(raw_data):
        return transform(raw_data)

    @task
    def load_task(clean_users):
        load(clean_users)


#Execution order and data flow
    raw = extract_task()
    cleaned = transform_task(raw)
    load_task(cleaned)

#Instantiate the data for airflow 
users_pipeline()

RAW_DIR = "$HOME/airflow/data/raw"   # raw files live outside dags/ so the dag-processor ignores them
# Date of the run: the logical date, or the trigger time for manual runs that have none
# (dag_run.logical_date is None for such runs; the bare `logical_date` variable is missing entirely)
RUN_DATE = "(dag_run.logical_date or dag_run.run_after)"


@dag(
    dag_id="users_pipeline_bash",
    start_date=datetime(2026, 11, 20),
    schedule="@daily",
    catchup=False,
    tags=["etl", "lux", "bash"]
)
def users_pipeline_bash():

    # Use bash operators to check the project files, where they are and where we expect them
    check_files = BashOperator(
        task_id="check_files",
        bash_command=f"cd {PROJ_DIR} && ls etl_pipeline.py .env && echo 'Project files found'"
    )

    #2. Bash operator to run airflow orchestration as we would in terminal
    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=f"cd {PROJ_DIR} && python etl_pipeline.py"
    )
    
    #3. Let's get a message to see the run dates of the pipeline
    last_update = BashOperator(
        task_id="last_update",
        bash_command="echo 'Pipeline last update for {{ " + RUN_DATE + ".strftime('%Y-%m-%d') }}'"
    )

    #Skipping saturday and sunday (exit code 99 = BashOperator marks the task as skipped)
    weekdays_only = BashOperator(
        task_id="weekdays_only",
        bash_command=(
            "DAY={{ " + RUN_DATE + ".strftime('%u') }};"   # 1=Mon ... 7=Sun
            "echo \"Day of the week: $DAY\";"
            "if [ \"$DAY\" -ge 6 ]; then echo 'weekend: skipping'; exit 99; fi"
        ),
    )

    #Compress raw files older than 7 days and delete compressed files older than 30 days
    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=(
            # stop here if the folder can't be entered, so find never runs in the wrong place
            f"mkdir -p {RAW_DIR} && cd {RAW_DIR} || exit 1;"
            "find . -name '*.json' -mtime +7 -exec gzip -f {} + ;"
            "find . -name '*.json.gz' -mtime +30 -delete ; "
            "echo \"Files Kept: $(ls | wc -l)\" "
        ),
        trigger_rule="none_failed",
    )

    # weekday check comes before the load, so weekends skip run_pipeline and last_update;
    # clean_up (trigger_rule="none_failed") still runs after a skip
    check_files >> weekdays_only >> run_pipeline >> last_update >> clean_up

users_pipeline_bash()