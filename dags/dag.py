from airflow import DAG
from airflow.operators.python import PythonOperator
from src.extract_weather import extract_data
from src.blob_runner import upload_blob
from datetime import datetime, timedelta

default_args = {
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_weather_dag",
    description="Extracts weather data and uploads to Azure blob storage.",
    start_date=datetime(2025, 1, 1),
    schedule="00 16 * * *",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["weather", "azure", "etl"],
    default_args=default_args,
) as dag:

    task_extract_weather = PythonOperator(
        dag=dag, task_id="run_extract_weather", python_callable=extract_data
    )
    task_blob_runner = PythonOperator(
        dag=dag, task_id="run_blob_runner", python_callable=upload_blob
    )

task_extract_weather >> task_blob_runner
