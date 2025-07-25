from src.helper import setup_logger
import logging
from dotenv import load_dotenv, dotenv_values
import snowflake.connector as sf
from pathlib import Path
from airflow.decorators import task
from datetime import datetime

logger = logging.getLogger("load_to_snowflake")


def get_snowflake_auth_data() -> dict:
    try:
        if not Path(".env").exists():
            raise FileNotFoundError(".env file not found")
        load_dotenv()
        dotenv = dotenv_values()

        mandatory_dotenv_keys = (
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_ACCOUNT",
        )
        for key in mandatory_dotenv_keys:
            # Check that .env has mandatory keys
            if key not in dotenv.keys():
                raise KeyError(f"{key} key is missing from .env")
            # Check that mandatory keys are not empty
            if not dotenv[key]:
                raise ValueError(f"{key} value is empty")

        auth_data = {
            "user": dotenv["SNOWFLAKE_USER"],
            "password": dotenv["SNOWFLAKE_PASSWORD"],
            "account": dotenv["SNOWFLAKE_ACCOUNT"],
        }
        if "SNOWFLAKE_WAREHOUSE" in dotenv.keys():
            auth_data["warehouse"] = dotenv["SNOWFLAKE_WAREHOUSE"]
        if "SNOWFLAKE_DATABASE" in dotenv.keys():
            auth_data["database"] = dotenv["SNOWFLAKE_DATABASE"]
        if "SNOWFLAKE_SCHEMA" in dotenv.keys():
            auth_data["schema"] = dotenv["SNOWFLAKE_SCHEMA"]

    except Exception as e:
        logger.exception("Failed to load authentication data: %s", e)
        raise
    else:
        logger.info("Authentication data loaded")
    return auth_data


def get_connection(auth_data: dict) -> sf.connection.SnowflakeConnection:
    try:
        mandatory_keys = ("user", "password", "account")
        for key in mandatory_keys:
            # Check that mandatory auth keys are present
            if key not in auth_data.keys():
                raise KeyError(f"{key} key is missing")
            # Check that mandatory auth data is not empty
            if not auth_data[key]:
                raise ValueError(f"{key} value is empty")

        logger.debug(
            "Auth data: USER:'%s', PASSWORD:'%s', ACCOUNT: '%s'",
            auth_data["user"],
            auth_data["password"],
            auth_data["account"],
        )
        sf_conn = sf.connect(**auth_data)
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

        result = sf_cursor.fetchall()
    except Exception as e:
        logger.exception("Failed to execute SQL instruction: %s", e)
        raise
    else:
        logger.info("SQL instruction executed")
    return result


def build_copy_sql(file: str) -> str:
    return f"""
    COPY INTO weather_data
    FROM @azure_weather_stage/{file}
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    """


def check_stage_file_rows(sf_cursor: sf.cursor, file: str) -> int:
    sql_row_count = f"""
    SELECT count(*)
    FROM @azure_weather_stage/{file}
    """
    result = execute_sql(sf_cursor, sql_row_count)
    return result[0][0]


def check_table_rows(sf_cursor: sf.cursor) -> int:
    sql_row_count = "SELECT count(*) FROM weather_data"
    result = execute_sql(sf_cursor, sql_row_count)
    return result[0][0]


def load_to_snowflake_logic(uploaded_file: str = None) -> None:
    setup_logger()
    logger.info("--- Load to Snowflake started ---")

    auth_data = get_snowflake_auth_data()
    if uploaded_file is None:
        uploaded_file = f'weather_{datetime.today().strftime("%Y%m%d")}.csv'
    with get_connection(auth_data) as sf_conn:
        with get_cursor(sf_conn) as sf_cursor:
            logger.debug(
                "Rows to be uploaded: %s",
                check_stage_file_rows(sf_cursor, uploaded_file),
            )
            sql_copy = build_copy_sql(uploaded_file)
            execute_sql(sf_cursor, sql_copy)
            logger.debug(
                "'weather data' total rows after upload: %s",
                check_table_rows(sf_cursor),
            )

    logger.info("--- Load to Snowflake ended ---")


@task
def load_to_snowflake(uploaded_file: str = None) -> None:
    return load_to_snowflake_logic(uploaded_file)


if __name__ == "__main__":
    load_to_snowflake_logic(None)
