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
        logging.exception("Failed to setup logger: %s", e)
        raise


def save_to_csv(
    df: pd.DataFrame,
    file_prefix: str,
    output_dir: str,
    logger: Logger,
    save_index: bool = False,
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


def is_file_empty(filename: str) -> bool:
    with open(filename, mode="r", encoding="utf-8") as file:
        content = file.read().strip()
    return not content
