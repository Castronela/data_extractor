from src.helper import setup_logger, save_to_csv
import requests
import logging
import pandas as pd
from airflow.decorators import task
from airflow.operators.python import get_current_context

# Logger Setup
logger = logging.getLogger("extract_weather")


def fetch_weather_data(api_url: str) -> dict:
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.exception("Failed to fetch weather data: %s", e)
        raise
    else:
        logger.info("Weather data fetched")
    return response.json()


def extract_data_logic():
    setup_logger()
    logger.info("--- Weather data extraction started ---")

    api_url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"
    weather_json = fetch_weather_data(api_url)
    df = pd.DataFrame(weather_json)

    try:
        save_date = get_current_context()["logical_date"]
    except RuntimeError:
        save_date = None
    filename = save_to_csv(
        df,
        "weather",
        "data/raw",
        execution_date=save_date,
        logger=logger,
        save_index=True,
    )

    logger.info("--- Weather data extraction ended ---")
    return filename


@task
def extract_data():
    return extract_data_logic()


if __name__ == "__main__":
    extract_data_logic()
