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