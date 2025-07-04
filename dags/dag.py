from airflow import DAG
from airflow.operators.python import PythonOperator
from src.extract_weather import extract_data
from datetime import datetime

with DAG(
    dag_id="extract_weather_dag",
    start_date=datetime(2025, 1, 1),
    schedule="00 16 * * *",
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    task = PythonOperator(task_id="run_extract_weather", python_callable=extract_data)
