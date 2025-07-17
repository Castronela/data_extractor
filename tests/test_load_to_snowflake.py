from src.load_to_snowflake import (
    get_snowflake_auth_data,
    get_connection,
    get_cursor,
    execute_sql,
)
from src.helper import setup_logger
from helper import check_for_raised_exception
import logging
import shutil
import os
import pytest
from pathlib import Path
from unittest.mock import Mock, patch

test_logger = logging.getLogger(__name__)
setup_logger()


class TestGetSnowflakeAuthData:

    target_function = "get_snowflake_auth_data"

    # Test if function handles missing .env file, by raising FileNotFoundError
    def test_missing_env_file(self, tmp_path):
        description = f"{self.target_function:<30}: Test for handling missing .env"

        # Backup and then remove .env file from cwd
        env_cwd_path = Path.cwd() / ".env"
        env_tmp_path = tmp_path / ".env"
        if env_cwd_path.is_file():
            shutil.copy(env_cwd_path, env_tmp_path)
            os.remove(env_cwd_path)

        # Test for exception raised
        check_for_raised_exception(
            FileNotFoundError, description, test_logger, get_snowflake_auth_data
        )

        # Restore original .env file
        if env_tmp_path.is_file():
            shutil.copy(env_tmp_path, env_cwd_path)

    @pytest.mark.parametrize(
        "dotenv",
        [
            {"SNOWFLAKE_PASSWORD": "password", "SNOWFLAKE_ACCOUNT": "account"},
            {"SNOWFLAKE_USER": "user", "SNOWFLAKE_ACCOUNT": "account"},
            {"SNOWFLAKE_USER": "user", "SNOWFLAKE_PASSWORD": "password"},
        ],
        ids=["missing user", "missing password", "missing account"],
    )
    def test_missing_dotenv_keys(self, dotenv, request) -> None:
        test_id = request.node.callspec.id
        description = f"{self.target_function:<30}: Test for handling missing .env keys: {test_id}"

        with patch("src.load_to_snowflake.load_dotenv") as mock_load_dotenv, patch(
            "src.load_to_snowflake.dotenv_values"
        ) as mock_dotenv_values:
            mock_load_dotenv.return_value = None
            mock_dotenv_values.return_value = dotenv
            check_for_raised_exception(
                KeyError, description, test_logger, get_snowflake_auth_data
            )

    @pytest.mark.parametrize(
        "user, password, account",
        [
            ("", "password", "account"),
            ("user", "", "account"),
            ("user", "password", ""),
        ],
        ids=["missing user", "missing password", "missing account"],
    )
    def test_empty_dotenv_credentials(self, user, password, account, request) -> None:
        test_id = request.node.callspec.id
        description = f"{self.target_function:<30}: Test for handling empty .env credentials: {test_id}"

        with patch("src.load_to_snowflake.load_dotenv") as mock_load_dotenv, patch(
            "src.load_to_snowflake.dotenv_values"
        ) as mock_dotenv_values:
            mock_load_dotenv.return_value = None
            mock_dotenv_values.return_value = {
                "SNOWFLAKE_USER": user,
                "SNOWFLAKE_PASSWORD": password,
                "SNOWFLAKE_ACCOUNT": account,
            }
            check_for_raised_exception(
                ValueError, description, test_logger, get_snowflake_auth_data
            )


class TestGetConnection:

    target_function = "get_connection"

    @pytest.mark.parametrize(
        "auth_data",
        [
            {"password": "password", "account": "account"},
            {"user": "user", "account": "account"},
            {"user": "user", "password": "password"},
        ],
        ids=["missing user", "missing password", "missing account"],
    )
    def test_missing_auth_keys(self, auth_data, request) -> None:
        test_id = request.node.callspec.id
        description = f"{self.target_function:<30}: Test for handling missing auth keys: {test_id}"

        with patch("src.load_to_snowflake.sf") as mock_sf:
            mock_sf_conn = Mock()
            mock_sf.connect.return_value = mock_sf_conn
            check_for_raised_exception(
                KeyError, description, test_logger, get_connection, auth_data
            )

    @pytest.mark.parametrize(
        "user, password, account",
        [
            ("", "password", "account"),
            ("user", "", "account"),
            ("user", "password", ""),
        ],
        ids=["missing user", "missing password", "missing account"],
    )
    def test_empty_auth_credentials(self, user, password, account, request) -> None:
        test_id = request.node.callspec.id
        description = f"{self.target_function:<30}: Test for handling empty auth credentials: {test_id}"

        auth_data = {
            "user": user,
            "password": password,
            "account": account,
        }
        with patch("src.load_to_snowflake.sf") as mock_sf:
            mock_sf_conn = Mock()
            mock_sf.connect.return_value = mock_sf_conn
            check_for_raised_exception(
                ValueError, description, test_logger, get_connection, auth_data
            )

    @patch("src.load_to_snowflake.sf")
    def test_correct_return(self, mock_sf) -> None:
        description = (
            f"{self.target_function:<30}: Test for checking correct return value"
        )

        auth_data = {
            "user": "user",
            "password": "password",
            "account": "account",
        }
        mock_sf_conn = Mock()
        mock_sf.connect.return_value = mock_sf_conn
        try:
            result = get_connection(auth_data)
            assert result == mock_sf_conn
        except AssertionError:
            test_logger.exception("FAILED: %s", description)
        else:
            test_logger.info("PASSED: %s", description)

    @patch("src.load_to_snowflake.sf")
    def test_failed_sf_connection(self, mock_sf) -> None:
        description = (
            f"{self.target_function:<30}: Test for handling failed snowflake connection"
        )

        auth_data = {
            "user": "user",
            "password": "password",
            "account": "account",
        }
        mock_sf_conn = Mock()
        mock_sf.connect.return_value = mock_sf_conn
        mock_sf.connect.side_effect = Exception
        check_for_raised_exception(
            Exception, description, test_logger, get_connection, auth_data
        )


class TestGetCursor:

    target_function = "get_cursor"

    def test_correct_return(self) -> None:
        description = (
            f"{self.target_function:<30}: Test for checking correct return value"
        )

        mock_sf_conn = Mock()
        mock_sf_cursor = Mock()
        mock_sf_conn.cursor.return_value = mock_sf_cursor
        try:
            result = get_cursor(mock_sf_conn)
            assert result == mock_sf_cursor
        except AssertionError:
            test_logger.exception("FAILED: %s", description)
        else:
            test_logger.info("PASSED: %s", description)

    def test_failed_cursor(self) -> None:
        description = (
            f"{self.target_function:<30}: Test for handling failed cursor obj retrieval"
        )

        mock_sf_conn = Mock()
        mock_sf_cursor = Mock()
        mock_sf_conn.cursor.return_value = mock_sf_cursor
        mock_sf_conn.cursor.side_effect = Exception
        check_for_raised_exception(
            Exception, description, test_logger, get_cursor, mock_sf_conn
        )


class TestExecuteSql:
    target_function = "execute_sql"

    def test_empty_sql_instruction(self) -> None:
        description = (
            f"{self.target_function:<30}: Test for handling empty sql instruction"
        )

        mock_sf_cursor = Mock()
        mock_sf_cursor.execute = lambda: None
        empty_sql_instruction = ""
        check_for_raised_exception(
            ValueError,
            description,
            test_logger,
            execute_sql,
            mock_sf_cursor,
            empty_sql_instruction,
        )

    def test_failed_sql_exec(self) -> None:
        description = f"{self.target_function:<30}: Test for handling failed sql instruction execution"

        mock_sf_cursor = Mock()
        mock_sf_cursor.execute.side_effect = Exception
        mock_sf_cursor.execute = lambda: None
        sql_instruction = "sql instruction"
        check_for_raised_exception(
            Exception,
            description,
            test_logger,
            execute_sql,
            mock_sf_cursor,
            sql_instruction,
        )
