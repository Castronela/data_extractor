import logging
from logging.config import dictConfig
from logging import Logger
import json
from pathlib import Path
from datetime import datetime
import pandas as pd


def setup_logger():
    config_path = "config/logging.json"
    try:
        if not Path(config_path).exists():
            raise FileNotFoundError(f"file {config_path} not found")
        with open(config_path, encoding="utf-8") as file:
            config = json.load(file)
        dictConfig(config)
    except Exception as e:
        print("Failed to setup logger: %s", e)
        print("Using default logging config")
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
        )


def save_to_csv(
    df: pd.DataFrame,
    file_prefix: str,
    output_dir: str,
    execution_date: datetime = None,
    logger: Logger | None = None,
    save_index: bool = False,
) -> str:
    if execution_date is not None:
        date = execution_date.strftime("%Y%m%d")
    else:
        date = datetime.today().strftime("%Y%m%d")
    filename = f"{output_dir}/{file_prefix}_{date}.csv"
    try:
        df.to_csv(filename, index=save_index)
    except Exception:
        if logger:
            logger.exception("Failed to save to '%s'", filename)
        raise
    else:
        if logger:
            logger.info("Saved csv to '%s'", filename)
    return filename


def is_file_empty(filename: str) -> bool:
    with open(filename, mode="r", encoding="utf-8") as file:
        content = file.read().strip()
    return not content
