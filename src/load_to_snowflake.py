import logging
import json
import logging.config
from dotenv import load_dotenv, dotenv_values
import snowflake.connector as sf
from pathlib import Path
from datetime import datetime

logger = logging.getLogger("load_to_snowflake")


def setup_logger(func):
    def wrapper():
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
        return func()

    return wrapper


def get_snowflake_auth_data() -> dict:
    try:
        if not Path(".env").exists():
            raise FileNotFoundError(".env file not found")
        load_dotenv()
        dotenv = dotenv_values()
        auth_data = {
            "user": dotenv["SNOWFLAKE_USER"],
            "password": dotenv["SNOWFLAKE_PASSWORD"],
            "account": dotenv["SNOWFLAKE_ACCOUNT"],
            "warehouse": dotenv["SNOWFLAKE_WAREHOUSE"],
            "database": dotenv["SNOWFLAKE_DATABASE"],
            "schema": dotenv["SNOWFLAKE_SCHEMA"],
        }

        # Check that mandatory auth data is not empty
        if not auth_data["user"]:
            raise ValueError("user value is empty")
        if not auth_data["password"]:
            raise ValueError("password value is empty")
        if not auth_data["account"]:
            raise ValueError("account value is empty")
    except Exception as e:
        logger.exception("Failed to load authentication data: %s", e)
        raise
    else:
        logger.info("Authentication data loaded")
    return auth_data


def get_connection(auth_data: dict) -> sf.connection.SnowflakeConnection:
    try:
        # Check that mandatory auth data is not empty
        if not auth_data["user"]:
            raise ValueError("user value is empty")
        if not auth_data["password"]:
            raise ValueError("password value is empty")
        if not auth_data["account"]:
            raise ValueError("account value is empty")

        logger.debug(
            "Auth data: USER:'%s', PASSWORD:'%s', ACCOUNT: '%s'",
            auth_data["user"],
            auth_data["password"],
            auth_data["account"],
        )
        sf_conn = sf.connect(
            user=auth_data["user"],
            password=auth_data["password"],
            account=auth_data["account"],
            warehouse=auth_data["warehouse"],
            database=auth_data["database"],
            schema=auth_data["schema"],
        )
    except Exception as e:
        logger.exception("Failed to connect to snowflake: %s", e)
        raise
    else:
        logger.info("Snowflake connection established")
    return sf_conn


def get_cursor(sf_conn: sf.connect) -> sf.cursor.SnowflakeCursor:
    try:
        sf_cursor = sf_conn.cursor()
    except Exception as e:
        logger.exception("Failed to retrieve snowflake cursor: %s", e)
        raise
    else:
        logger.info("Retrieved snowflake cursor")
    return sf_cursor


def execute_sql(sf_cursor: sf.cursor, instruction: str) -> None:
    try:
        if not instruction:
            raise ValueError("SQL instruction is empty")
        sf_cursor.execute(instruction)
        logger.debug("SQL instruction: %s", instruction)
    except Exception as e:
        logger.exception("Failed to execute SQL instruction: %s", e)
        raise
    else:
        logger.info("SQL instruction executed")


def build_copy_sql() -> str:
    return f"""
    COPY INTO weather_data
    FROM @azure_weather_stage/weather_{datetime.today().strftime("%Y%m%d")}.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    """


@setup_logger
def load_to_snowflake() -> None:
    logger.info("--- Load to Snowflake started ---")

    auth_data = get_snowflake_auth_data()
    with get_connection(auth_data) as sf_conn:
        with get_cursor(sf_conn) as sf_cursor:
            sql_copy = build_copy_sql()
            execute_sql(sf_cursor, sql_copy)

    logger.info("--- Load to Snowflake ended ---")


if __name__ == "__main__":
    load_to_snowflake()
