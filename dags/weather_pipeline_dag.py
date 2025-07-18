from airflow import DAG
from airflow.operators.python import PythonOperator
from src.extract_weather import extract_data
from src.transform_weather import transform_data
from src.blob_runner import upload_blob
from src.load_to_snowflake import load_to_snowflake
from datetime import datetime, timedelta

default_args = {
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_weather_dag",
    description="Extracts weather data, transforms it, uploads the resulted csv file to Azure blob storage and load it to snowflake.",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["weather", "azure", "etl"],
    default_args=default_args,
) as dag:

    task_extract_weather = PythonOperator(
        dag=dag,
        task_id="run_extract_weather",
        python_callable=extract_data,
        op_kwargs={"execution_date": "{{ logical_date }}"},
    )
    task_transform_weather = PythonOperator(
        dag=dag,
        task_id="run_transform_weather",
        python_callable=transform_data,
        op_kwargs={"execution_date": "{{ logical_date }}"},
    )
    task_blob_runner = PythonOperator(
        dag=dag, task_id="run_blob_runner", python_callable=upload_blob
    )
    task_load_to_snowflake = PythonOperator(
        dag=dag, task_id="run_load_to_snowflake", python_callable=load_to_snowflake
    )

(
    task_extract_weather
    >> task_transform_weather
    >> task_blob_runner
    >> task_load_to_snowflake
)
