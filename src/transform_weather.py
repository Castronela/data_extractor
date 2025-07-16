import logging
import json
import csv
import pandas as pd
from extract_weather import save_to_csv
from pathlib import Path



logger = logging.getLogger("transform_weather")

def setup_logger(func):
    def wrapper(*args, **kwargs):
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
        return func(*args, **kwargs)
    return wrapper

def transform_hourly(data: pd.DataFrame) -> pd.DataFrame:
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
        hourly_df[index] = hourly[index].strip("[] ").replace("'", "").split(",")

    # Perform appropriate typecasts
    hourly_df = hourly_df.astype({time_unit: 'datetime64[ns]', temp_unit: 'float64'})

    return hourly_df


def transform_weather_data(data: pd.DataFrame) -> pd.DataFrame:
    hourly_df = transform_hourly(data)

    data.drop(columns=['hourly_units', 'hourly'], inplace=True)

    for column in data.columns:
        hourly_df[column] = data.iloc[0][column]
    return hourly_df

@setup_logger
def transform_data(*args, **kwargs) -> None:

    filename = "data/raw/weather_20250715.csv"
    df_input = pd.read_csv(filename)
    df_processed = transform_weather_data(df_input)
    print(df_processed.info())
    filename = save_to_csv(df_processed, "weather", "data/processed")
    
    return filename

if __name__ == "__main__":
    transform_data()