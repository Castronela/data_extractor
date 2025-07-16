import logging
import json
import pandas as pd
try:
    from .extract_weather import save_to_csv
except ImportError:
    from extract_weather import save_to_csv
from pathlib import Path
from datetime import datetime


logger = logging.getLogger("transform_weather")

def setup_logger(func):
    def wrapper(ti):
        try:
            config_path = "config/logging.json"
            if not Path(config_path).exists():
                raise FileNotFoundError(f"file {config_path} not found")
            with open(config_path, encoding="utf-8") as file:
                config = json.load(file)
            logging.config.dictConfig(config)
        except Exception as e:
            logging.exception("Failed to setup logger: %s", e)
            raise
        return func(ti)
    return wrapper

def is_file_empty(filename: str) -> bool:
    with open(filename, mode="r", encoding="utf-8") as file:
        content = file.read().strip()
    return not content

def get_filename_xcom(ti) -> str:
    try:
        filename = ti.xcom_pull(key="filename", task_ids="run_extract_weather")
    except Exception as e:
        logger.exception("Failed to retrieve file name from Xcom: %s", e)
        raise
    else:
        logger.info("Retrieved filename '%s' from Xcom", filename)
    return filename

def get_filename_manually(input_dir: str ="data/raw", file_prefix: str ="weather") -> str:
    today_str = datetime.today().strftime("%Y%m%d")
    filename = f"{input_dir}/weather_{today_str}.csv"
    return filename

def get_csv_df(filename: str) -> pd.DataFrame:  
    try:
        if not Path(filename).exists():
            raise FileNotFoundError("File '%s' not found" % (filename))
        if is_file_empty(filename):
            raise pd.errors.EmptyDataError("File '%s' is empty" % (filename))
        file_df = pd.read_csv(filename)
    except Exception as e:
        logger.exception("Failed to upload csv file data: %s", e)
    else:
        logger.info("Retrieved data from csv file '%s'", filename)
    return file_df

def process_hourly(data: pd.DataFrame) -> pd.DataFrame:
    # Set first column as index and remove index name
    data.set_index(data.columns[0], inplace=True)
    data.index.name = ''

    # Append measurement unit to index values
    time_unit = f"time({data.at['time', 'hourly_units']})"
    temp_unit = f"temperature({data.at['temperature_2m', 'hourly_units']})"
    data.rename(index={'time': time_unit, 'temperature_2m': temp_unit}, inplace=True)
    
    # Format and convert 'hourly' collumn strings to list, and append to new dataframe 'hourly_df'
    hourly = data['hourly']
    hourly_df = pd.DataFrame(columns=hourly.index)
    for index in hourly.index:
        cleaned_values = [ val.strip() for val in hourly[index].strip("[] ").replace("'", "").split(",") ]
        hourly_df[index] = cleaned_values

    # Perform appropriate typecasts
    hourly_df = hourly_df.astype({time_unit: 'datetime64[ns]', temp_unit: 'float64'})

    return hourly_df

def process_weather_data(data: pd.DataFrame) -> pd.DataFrame:
    hourly_df = process_hourly(data)

    data.drop(columns=['hourly_units', 'hourly'], inplace=True)

    for column in data.columns:
        hourly_df[column] = data.iloc[0][column]
    return hourly_df

@setup_logger
def transform_data(ti) -> None:
    logger.info("--- Transforming weather data started ---")

    in_filename = get_filename_xcom(ti)
    csv_df = get_csv_df(in_filename)
    processed_df = process_weather_data(csv_df)
    out_filename = save_to_csv(processed_df, "weather", "data/processed")

    logger.info("--- Transforming weather data ended ---")
    return out_filename

@setup_logger
def transform_data_local(ti=None) ->None:
    logger.info("--- Transforming weather data started ---")

    in_filename = get_filename_manually()
    csv_df = get_csv_df(in_filename)
    processed_df = process_weather_data(csv_df)
    out_filename = save_to_csv(processed_df, "weather", "data/processed")

    logger.info("--- Transforming weather data ended ---")

if __name__ == "__main__":
    transform_data_local(None)