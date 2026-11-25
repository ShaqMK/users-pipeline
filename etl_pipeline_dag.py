from datetime import datetime
from airflow.sdk import dag, task
from etl_pipeline import extract, transform, load

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

def users_pipeline():

    # Use bash operators to check the project files, where they are and where we expect them
    check_files = BashOperator(
        task_id="check_files",
        bash_command=f"cd {PROJ_DIR} && ls etl_pipeline.py .env $$ echo 'Project files found'"
    )

    #2. Bash operator to run airflow orchestration as we would in terminal
    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=f"cd {PROJ_DIR} && python etl_pipeline.py"
    )
    
    #3. Let's get a message to see the run dates of the pipeline
    last_update = BashOperator(
        task_is="last_update",
        bash_command="echo 'Pipeline last update for {{ ds }}"
    )

    check_files >> run_pipeline >> last_update

users_pipeline()