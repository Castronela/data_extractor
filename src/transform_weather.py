import logging
import pandas as pd

try:
    from .helper import setup_logger, save_to_csv, is_file_empty
except ImportError:
    from helper import setup_logger, save_to_csv, is_file_empty
from pathlib import Path
from datetime import datetime


logger = logging.getLogger("transform_weather")
setup_logger()


def get_filename_xcom(ti) -> str:
    try:
        filename = ti.xcom_pull(key="filename", task_ids="run_extract_weather")
    except Exception as e:
        logger.exception("Failed to retrieve file name from Xcom: %s", e)
        raise
    else:
        logger.info("Retrieved filename '%s' from Xcom", filename)
    return filename


def get_filename_manually(
    input_dir: str = "data/raw", file_prefix: str = "weather"
) -> str:
    today_str = datetime.today().strftime("%Y%m%d")
    filename = f"{input_dir}/{file_prefix}_{today_str}.csv"
    return filename


def get_csv_df(filename: str) -> pd.DataFrame:
    try:
        if not Path(filename).exists():
            raise FileNotFoundError("File '%s' not found" % (filename))
        if is_file_empty(filename):
            raise pd.errors.EmptyDataError("File '%s' is empty" % (filename))
        file_df = pd.read_csv(filename)
    except Exception as e:
        logger.exception("Failed to import csv file data: %s", e)
        raise
    else:
        logger.info("Retrieved data from csv file '%s'", filename)
    return file_df


def process_hourly(data: pd.DataFrame) -> pd.DataFrame:
    # Set first column as index and remove index name
    data.set_index(data.columns[0], inplace=True)
    data.index.name = ""

    # Append measurement unit to index values
    time_unit = f"time({data.at['time', 'hourly_units']})"
    temp_unit = f"temperature({data.at['temperature_2m', 'hourly_units']})"
    data.rename(index={"time": time_unit, "temperature_2m": temp_unit}, inplace=True)

    # Format and convert 'hourly' collumn strings to list, and append to new dataframe 'hourly_df'
    hourly = data["hourly"]
    hourly_df = pd.DataFrame(columns=hourly.index)
    for index in hourly.index:
        cleaned_values = [
            val.strip()
            for val in hourly[index].strip("[] ").replace("'", "").split(",")
        ]
        hourly_df[index] = cleaned_values

    # Perform appropriate typecasts
    hourly_df = hourly_df.astype({time_unit: "datetime64[ns]", temp_unit: "float64"})

    return hourly_df


def process_weather_data(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        logger.error("Failed to process weather data: Dataframe is empty")
        raise ValueError("Dataframe 'data' is empty")
    hourly_df = process_hourly(data)

    data.drop(columns=["hourly_units", "hourly"], inplace=True)

    for column in data.columns:
        hourly_df[column] = data.iloc[0][column]
    logger.info("Weather data processed")
    return hourly_df


def transform_data(ti) -> None:
    setup_logger()
    logger.info("--- Transforming weather data started ---")

    in_filename = get_filename_xcom(ti)
    csv_df = get_csv_df(in_filename)
    processed_df = process_weather_data(csv_df)
    out_filename = save_to_csv(processed_df, "weather", "data/processed", logger)

    logger.info("--- Transforming weather data ended ---")
    return out_filename


def transform_data_local(ti=None) -> None:
    setup_logger()
    logger.info("--- Transforming weather data started ---")

    _ = ti
    in_filename = get_filename_manually()
    csv_df = get_csv_df(in_filename)
    processed_df = process_weather_data(csv_df)
    out_filename = save_to_csv(processed_df, "weather", "data/processed", logger)

    logger.info("--- Transforming weather data ended ---")
    return out_filename


if __name__ == "__main__":
    transform_data_local(None)
