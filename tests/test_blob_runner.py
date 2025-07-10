from src.blob_runner import (
    get_dotenv_auth_data,
    get_container_client,
    get_files_paths_to_upload,
    upload_files_to_container,
)
import logging
import json
import shutil
import os
import pytest
from pathlib import Path
from unittest.mock import Mock, patch

test_logger = logging.getLogger(__name__)


def setup_logging():
    config_file = "config/logging.json"
    try:
        with open(config_file, encoding="utf-8") as file:
            config = json.load(file)
        logging.config.dictConfig(config)
    except Exception as e:
        logging.exception("Logging setup failed: %s", e)
        raise


setup_logging()


def check_for_raised_exception(
    exception: Exception, description: str, func, *args, **kwargs
):
    raised = False
    try:
        func(*args, **kwargs)
    except exception:
        raised = True
    except Exception:
        pass
    if raised:
        test_logger.info("PASSED: %s", description)
    else:
        test_logger.info("FAILED: %s", description)
        assert False, f"Expected {exception}"


class TestGetDotenvAuthData:

    target_function = "get_dotenv_auth_data"

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
        check_for_raised_exception(FileNotFoundError, description, get_dotenv_auth_data)

        # Restore original .env file
        if env_tmp_path.is_file():
            shutil.copy(env_tmp_path, env_cwd_path)

    # Test if function handles missing keys, by raising KeyError
    def test_missing_key(self, tmp_path):
        description = f"{self.target_function:<30}: Test for handling missing keys"

        # Backup and then truncate .env file
        env_cwd_path = Path.cwd() / ".env"
        env_tmp_path = tmp_path / ".env"
        if env_cwd_path.is_file():
            shutil.copy(env_cwd_path, env_tmp_path)
        with open(env_cwd_path, "w", encoding="utf-8") as file:
            file.write("")

        # Test for exception raised
        check_for_raised_exception(KeyError, description, get_dotenv_auth_data)

        # Restore original .env file
        if env_tmp_path.is_file():
            shutil.copy(env_tmp_path, env_cwd_path)

    # Test if function handles missing auth value, by raising ValueError
    @pytest.mark.parametrize(
        "dotenv_content",
        [
            "AZURE_STORAGE_CONNECTION_STRING=connection_str\nAZURE_BLOB_CONTAINER_ID=",
            "AZURE_STORAGE_CONNECTION_STRING=\nAZURE_BLOB_CONTAINER_ID=container_id",
        ],
    )
    def test_missing_value(self, tmp_path, dotenv_content):
        description = (
            f"{self.target_function:<30}: Test for handling missing auth value"
        )

        # Backup and then overwrite .env file with key only
        env_cwd_path = Path.cwd() / ".env"
        env_tmp_path = tmp_path / ".env"
        if env_cwd_path.is_file():
            shutil.copy(env_cwd_path, env_tmp_path)
        with open(env_cwd_path, "w", encoding="utf-8") as file:
            file.write(dotenv_content)

        # Test for exception raised
        check_for_raised_exception(ValueError, description, get_dotenv_auth_data)

        # Restore original .env file
        if env_tmp_path.is_file():
            shutil.copy(env_tmp_path, env_cwd_path)


class TestGetContainerClient:

    target_function = "get_container_client"

    # Test if function handles valid connection string
    @patch("src.blob_runner.BlobServiceClient")
    def test_valid_args(self, mock_BlobServiceClient):
        description = f"{self.target_function:<30}: Test for handling valid arguments"
        mock_blob_service_client = Mock()
        mock_container_client = Mock()
        mock_BlobServiceClient.from_connection_string.return_value = (
            mock_blob_service_client
        )
        mock_blob_service_client.get_container_client.return_value = (
            mock_container_client
        )
        auth_data = {
            "connection_str": "good_connection_string",
            "container_id": "good_container_id",
        }
        try:
            result = get_container_client(auth_data)
            assert result == mock_container_client
        except AssertionError:
            test_logger.exception("FAILED: %s", description)
            raise
        else:
            test_logger.info("PASSED: %s:", description)

    # Test if function handles invalid connection string
    @patch("src.blob_runner.BlobServiceClient")
    def test_invalid_args(self, mock_BlobServiceClient):
        description = f"{self.target_function:<30}: Test for handling invalid arguments"
        mock_blob_service_client = Mock()
        mock_container_client = Mock()
        mock_BlobServiceClient.from_connection_string.return_value = (
            mock_blob_service_client
        )
        mock_blob_service_client.get_container_client.return_value = (
            mock_container_client
        )
        mock_blob_service_client.get_container_client.side_effect = Exception
        auth_data = {
            "connection_str": "good_connection_string",
            "container_id": "good_container_id",
        }

        check_for_raised_exception(
            Exception, description, get_container_client, auth_data
        )

    # Test if function handles empty connection string / container id
    @pytest.mark.parametrize(
        "connection_str, container_id", [("", "container_id"), ("connection_str", "")]
    )
    @patch("src.blob_runner.BlobServiceClient")
    def test_empty_container_id_or_connection_str(
        self, mock_BlobServiceClient, connection_str, container_id
    ):
        empty_arg = "connection string" if not connection_str else "container id"
        description = f"{self.target_function:<30}: Test for handling empty {empty_arg}"

        mock_blob_service_client = Mock()
        mock_container_client = Mock()
        mock_BlobServiceClient.from_connection_string.return_value = (
            mock_blob_service_client
        )
        mock_blob_service_client.get_container_client.return_value = (
            mock_container_client
        )
        auth_data = {"connection_str": connection_str, "container_id": container_id}
        check_for_raised_exception(
            ValueError, description, get_container_client, auth_data
        )


class TestGetFilesPathsToUpload:

    target_function = "get_files_paths_to_upload"

    # Test if function returns only .csv files when option only_csv is True
    def test_only_csv(self, tmp_path):
        description = f"{self.target_function:<30}: Test for returning only .csv files when option only_csv is True"

        files = ("good.csv", "bad.txt", "bad.csvv")
        file_paths = [str(tmp_path / f) for f in files]
        for fp in file_paths:
            open(fp, "a", encoding="utf-8").close()

        only_csv = True
        result = get_files_paths_to_upload(source_path=tmp_path, only_csv=only_csv)
        try:
            assert len(result) == 1
            assert result[0] == str(tmp_path / "good.csv")
        except AssertionError:
            test_logger.exception("FAILED: %s", description)
            raise
        else:
            test_logger.info("PASSED: %s", description)

    # Test if function returns all files when option only_csv is False
    def test_all_files(self, tmp_path):
        description = f"{self.target_function:<30}: Test for returning all files when option only_csv is False"

        files = ("good.csv", "good.txt", "good.csvv")
        file_paths = [str(tmp_path / f) for f in files]
        for fp in file_paths:
            open(fp, "a", encoding="utf-8").close()

        only_csv = False
        result = get_files_paths_to_upload(source_path=tmp_path, only_csv=only_csv)
        try:
            assert len(result) == 3, f"Expected 3 files, got {len(result)}"
            for item in result:
                assert item in file_paths
        except AssertionError:
            test_logger.exception("FAILED: %s", description)
            raise
        else:
            test_logger.info("PASSED: %s", description)

    # Test if function handles invalid source path
    def test_invalid_source_path(self):
        description = (
            f"{self.target_function:<30}: Test for handling invalid source path"
        )

        bad_path = "/dev/null/subdir"
        check_for_raised_exception(
            Exception, description, get_files_paths_to_upload, bad_path
        )


class TestUploadFilesToContainer:

    target_function = "upload_files_to_container"

    # Test if function handles invalid file path
    def test_invalid_file_path(self):
        description = (
            f"{self.target_function:<30}: Test for handling invalid file path to upload"
        )

        bad_file_path = ["/dev/null/bad.csv"]
        mock_container_client = Mock()
        mock_blob = Mock()
        mock_container_client.upload_blob.return_value = mock_blob
        mock_blob.get_blob_properties.return_value = {}
        check_for_raised_exception(
            Exception,
            description,
            upload_files_to_container,
            mock_container_client,
            bad_file_path,
        )

    # Test if function handles upload failure
    def test_upload_failure(self, tmp_path):
        description = f"{self.target_function:<30}: Test for handling upload failure"

        test_file = tmp_path / "test.csv"
        open(test_file, "a", encoding="utf-8").close()
        test_file_path = [str(test_file)]

        mock_container_client = Mock()
        mock_container_client.upload_blob.side_effect = Exception

        check_for_raised_exception(
            Exception,
            description,
            upload_files_to_container,
            mock_container_client,
            test_file_path,
        )
