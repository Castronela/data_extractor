import logging
import pandas as pd
from src.helper import setup_logger, save_to_csv, is_file_empty
from pathlib import Path
from datetime import datetime
from airflow.decorators import task
from airflow.operators.python import get_current_context


logger = logging.getLogger("transform_weather")


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
    dtype_map = {}
    for index in data.index:
        index_w_unit = f"{index}({data.at[index, 'hourly_units']})"
        data.rename(index={index: index_w_unit}, inplace=True)
        if "time" in index.lower():
            dtype_map[index_w_unit] = "datetime64[ns]"
        else:
            dtype_map[index_w_unit] = "float64"

    # Format and convert 'hourly' collumn strings to list, and append to new dataframe 'hourly_df'
    hourly = data["hourly"]
    hourly_df = pd.DataFrame(columns=hourly.index)
    for index in hourly.index:
        cleaned_values = [
            val.strip("' ") for val in hourly[index].strip("[]").split(",")
        ]
        hourly_df[index] = cleaned_values

    # Perform appropriate typecasts
    hourly_df = hourly_df.astype(dtype_map)

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


def transform_data_logic(in_filename: str = None) -> None:
    setup_logger()
    logger.info("--- Transforming weather data started ---")

    if in_filename is None:
        in_filename = get_filename_manually()
    csv_df = get_csv_df(in_filename)
    processed_df = process_weather_data(csv_df)
    try:
        save_date = get_current_context()["logical_date"]
    except RuntimeError:
        save_date = None
    out_filename = save_to_csv(
        processed_df,
        "weather",
        "data/processed",
        execution_date=save_date,
        logger=logger,
    )

    logger.info("--- Transforming weather data ended ---")
    return out_filename


@task
def transform_data(in_filename: str = None) -> None:
    return transform_data_logic(in_filename)


if __name__ == "__main__":
    transform_data_logic(None)
