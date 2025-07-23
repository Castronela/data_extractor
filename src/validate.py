import pandas as pd
from src.helper import setup_logger
import logging
from datetime import datetime
from airflow.decorators import task

logger = logging.getLogger("validate")


def validate_transform_data_logic(file: str = None) -> None:
    setup_logger()
    logger.info("--- Validation started ---")
    if file is None:
        file = f'data/processed/weather_{datetime.today().strftime("%Y%m%d")}.csv'
    df = pd.read_csv(file)
    validate(df)
    logger.info("--- Validation ended ---")


def validate(df: pd.DataFrame) -> None:
    # Check for empty DataFrame
    if df.empty:
        logger.error("FAILED Validation: empty DataFrame")
        raise ValueError("DataFrame is empty")
    logger.info("PASSED Validation: empty DataFrame")

    # Check for columns 'time' and 'temperature'
    has_time = df.columns.str.match(r"^time[^A-Za-z]+.*", case=False).any()
    has_temperature = df.columns.str.match(
        r"^temperature[^A-Za-z]+.*", case=False
    ).any()
    if not has_time:
        logger.error("FAILED Validation: missing time column")
        raise ValueError("No time column found matching pattern 'time(...)'")
    logger.info("PASSED Validation: missing time column")
    if not has_temperature:
        logger.error("FAILED Validation: missing temperature column")
        raise ValueError(
            "No temperature column found matching pattern 'temperature(...)'"
        )
    logger.info("PASSED Validation: missing temperature column")

    # Check for null values
    if not df.notnull().all().all():
        logger.error("FAILED Validation: null values")
        raise ValueError("DataFrame has null values")
    logger.info("PASSED Validation: null values")

@task
def validate_transform_data(file):
    return validate_transform_data_logic(file)

if __name__ == "__main__":
    validate_transform_data_logic(None)
