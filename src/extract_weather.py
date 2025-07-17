import logging.config
import requests
import logging
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

# Logger Setup
logger = logging.getLogger("extract_weather")


def setup_logger():
    config_path = "config/logging.json"
    try:
        if not Path(config_path).exists():
            raise FileNotFoundError(f"file {config_path} not found")
        with open(config_path, encoding="utf-8") as file:
            config = json.load(file)
        logging.config.dictConfig(config)
    except Exception as e:
        logging.exception("Failed to setup logger: %s", e)
        raise


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


def save_to_csv(
    df: pd.DataFrame, file_prefix: str, output_dir: str, save_index: bool = False
) -> str:
    today_str = datetime.today().strftime("%Y%m%d")
    filename = f"{output_dir}/{file_prefix}_{today_str}.csv"
    try:
        df.to_csv(filename, index=save_index)
    except Exception:
        logger.exception("Failed to save to '%s'", filename)
        raise
    else:
        logger.info("Saved csv to '%s'", filename)
    return filename


def extract_data(ti=None):
    setup_logger()
    logger.info("--- Weather data extraction started ---")

    api_url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"
    weather_json = fetch_weather_data(api_url)
    df = pd.DataFrame(weather_json)
    filename = save_to_csv(df, "weather", "data/raw", save_index=True)
    if ti:
        ti.xcom_push(key="filename", value=filename)

    logger.info("--- Weather data extraction ended ---")


if __name__ == "__main__":
    extract_data(None)
