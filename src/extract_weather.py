import logging.config
import requests
import logging
import json
import pandas as pd
from datetime import datetime

# Logger Setup
logger = logging.getLogger("logger")


def setup_logging():
    config_file = "config/logging.json"
    try:
        with open(config_file, encoding="utf-8") as file:
            config = json.load(file)
    except FileNotFoundError:
        logger.exception("Logging config file '%s' does not exist", config_file)
        raise
    logging.config.dictConfig(config)


def fetch_weather_data(api_url: str) -> dict:
    logger.info("Fetching weather data")

    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        logger.exception("Failed to fetch weather data")
        raise

    logger.info("Weather data fetched successfully")
    return response.json()


def transform_weather_data(data: dict) -> pd.DataFrame:
    logger.info("Transforming weather data")

    hourly = data["hourly"]
    hourly_df = pd.DataFrame(hourly)

    metadata = data.copy()
    del metadata["hourly"]
    del metadata["hourly_units"]

    for key, value in metadata.items():
        hourly_df[key] = value
    return hourly_df


def save_to_csv(df: pd.DataFrame, output_dir="data") -> str:
    today_str = datetime.today().strftime("%Y%m%d")
    filename = f"{output_dir}/weather_{today_str}.csv"
    logger.debug("Saving weather data to %s", filename)
    df.to_csv(filename, index=False)
    return filename


def main():
    setup_logging()
    logger.info("Script started")
    api_url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"
    weather_json = fetch_weather_data(api_url)
    df = transform_weather_data(weather_json)
    filename = save_to_csv(df)
    print(f"Saved to {filename}")
    logger.info("Script ended")


if __name__ == "__main__":
    main()
