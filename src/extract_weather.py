import logging.config
import requests
import logging
import json
import pandas as pd
from datetime import datetime

# Logger Setup
logger = logging.getLogger("extract_weather")


def setup_logging(func):
    def wrapper():
        config_file = "config/logging.json"
        try:
            with open(config_file, encoding="utf-8") as file:
                config = json.load(file)
        except FileNotFoundError:
            logger.exception("Logging config file '%s' does not exist", config_file)
            raise
        logging.config.dictConfig(config)
        func()

    return wrapper


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


def save_to_csv(df: pd.DataFrame, file_prefix: str, output_dir: str, save_index: bool =False) -> str:
    today_str = datetime.today().strftime("%Y%m%d")
    filename = f"{output_dir}/{file_prefix}_{today_str}.csv"
    try:
        logger.debug("Saving csv to %s", filename)
        df.to_csv(filename, index=save_index)
    except Exception:
        logger.exception("Failed to save to %s", filename)
        raise
    else:
        logger.info("Saved csv to %s", filename)
    return filename

import numpy as np
@setup_logging
def extract_data():
    logger.info("--- Weather data extraction started ---")

    api_url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"
    weather_json = fetch_weather_data(api_url)
    df = pd.DataFrame(weather_json)
    filename = save_to_csv(df, "weather", "data/raw", save_index=True)

    logger.info("--- Weather data extraction ended ---")
    return filename


if __name__ == "__main__":
    extract_data()
