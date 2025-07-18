from src.helper import setup_logger, save_to_csv
import requests
import logging
import pandas as pd

# Logger Setup
logger = logging.getLogger("extract_weather")
setup_logger()


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


def extract_data(ti=None):
    setup_logger()
    logger.info("--- Weather data extraction started ---")

    api_url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"
    weather_json = fetch_weather_data(api_url)
    df = pd.DataFrame(weather_json)
    filename = save_to_csv(df, "weather", "data/raw", logger, save_index=True)
    if ti:
        ti.xcom_push(key="filename", value=filename)

    logger.info("--- Weather data extraction ended ---")


if __name__ == "__main__":
    extract_data(None)
