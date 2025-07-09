import logging.config
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv, dotenv_values
import logging
import json
import azure
from pathlib import Path

logger = logging.getLogger("blob_runner")


def setup_logging(func):
    def wrapper(*args, **kwargs):
        config_file = "config/logging.json"
        try:
            with open(config_file, encoding="utf-8") as file:
                config = json.load(file)
            logging.config.dictConfig(config)
        except Exception as e:
            logger.exception("Logging setup failed: %s", e)
            raise
        return func(*args, **kwargs)

    return wrapper


def get_connection_str() -> str:
    try:
        if not Path(".env").exists():
            raise FileNotFoundError(".env file not found")
        load_dotenv()
        auth = dotenv_values()
        connection_str = auth["AZURE_STORAGE_CONNECTION_STRING"]
        if not connection_str:
            raise ValueError("connection string empty")
    except (FileNotFoundError, ValueError, KeyError) as e:
        logger.exception("Failed to extract connection string from .env: %s", e)
        raise
    except Exception as e:
        logger.exception("Failed to load .env: %s", e)
        raise
    else:
        logger.info("Connection string loaded")
    return connection_str


def get_container_client(connection_string: str, container_id) -> ContainerClient:
    logger.info("Creating blob container object: %s", container_id)
    try:
        if not container_id:
            raise ValueError("container_id is empty")
        if not connection_string:
            raise ValueError("connection_string is empty")
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        container_client = blob_service_client.get_container_client(container_id)
    except Exception as e:
        logger.exception("Failed to create blob container object: %s", e)
        raise
    else:
        logger.info("Blob container object created")
    return container_client


# Extract all files from path
def get_files_paths_to_upload(source_path: str, only_csv=True) -> list:
    try:
        # Create list comprehension of all files within source_dir
        file_paths = [str(f) for f in Path(source_path).iterdir() if f.is_file()]
        # if only_csv is True, filter in only .csv files
        if only_csv:
            file_paths = [f for f in file_paths if f.endswith(".csv")]
    except Exception as e:
        logger.exception("Failed to retrieve files from dir '%s': %s", source_path, e)
        raise
    else:
        logger.debug("Files to upload: %s", file_paths)
    return file_paths


# Loop to upload each file as a blob
def upload_files_to_container(container_client: ContainerClient, file_paths: list):
    for file_path in file_paths:
        try:
            logger.info("Uploading '%s' to blob container", file_path)
            file_name = Path(file_path).name
            with open(file_path, "rb") as data:
                blob_client = container_client.upload_blob(
                    name=file_name, data=data, overwrite=False
                )
        except azure.core.exceptions.ResourceExistsError:
            logger.info("Blob already exists: %s", file_name)
        except Exception as e:
            logger.exception("Failed to upload '%s': %s", file_path, e)
            raise
        else:
            logger.debug("Blob properties: %s", blob_client.get_blob_properties())
            logger.info("File uploaded to blob container")


@setup_logging
def main():
    logger.info("--- Blob runner started ---")

    container_id = "weather-data"
    connection_str = get_connection_str()
    container_client = get_container_client(connection_str, container_id)
    file_paths = get_files_paths_to_upload("data")
    upload_files_to_container(container_client, file_paths)

    logger.info("--- Blob runner ended ---")


if __name__ == "__main__":
    main()
