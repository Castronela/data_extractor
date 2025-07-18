import logging
from logging.config import dictConfig
from logging import Logger
import json
from pathlib import Path
from datetime import datetime
import pandas as pd


def setup_logger(overwrite_config: bool = False):
    if not overwrite_config and logging.getLogger().handlers:
        return
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
    execution_date=None,
    logger: Logger | None = None,
    save_index: bool = False,
) -> str:
    if execution_date:
        date = str(datetime.fromisoformat(execution_date)).replace(" ", "_")
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


def get_xcom_data(ti, key: str, task_id: str, logger=None):
    try:
        data = ti.xcom_pull(key=key, task_ids=task_id)
    except Exception as e:
        if logger:
            logger.exception("Failed to retrieve data from Xcom: %s", e)
        raise
    else:
        if logger:
            logger.info("Retrieved data from Xcom: '%s'", data)
    return data
