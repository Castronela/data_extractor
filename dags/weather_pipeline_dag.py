from airflow.decorators import dag
from src.extract_weather import extract_data
from src.transform_weather import transform_data
from src.blob_runner import upload_blob
from src.load_to_snowflake import load_to_snowflake
from src.alerts import slack_failure_alert
from src.validate import validate_transform_data
from datetime import datetime, timedelta

default_args = {
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_alert,
}


@dag(
    dag_id="extract_weather_dag",
    description="Extracts weather data, transforms it, uploads the resulted csv file to Azure blob storage and load it to snowflake.",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["weather", "azure", "etl"],
    default_args=default_args,
)
def data_extractor():
    filename = extract_data()
    processed_file = transform_data(filename)
    validate_transform_data(processed_file)
    uploaded_files = upload_blob(processed_file)
    load_to_snowflake(uploaded_files)


dag = data_extractor()
