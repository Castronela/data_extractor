from src.helper import setup_logger
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv, dotenv_values
import logging
import azure
from pathlib import Path

logger = logging.getLogger("blob_runner")


def get_dotenv_auth_data() -> dict:
    try:
        if not Path(".env").exists():
            raise FileNotFoundError(".env file not found")
        load_dotenv()
        dotenv = dotenv_values()
        auth_data = {
            "connection_str": dotenv["AZURE_STORAGE_CONNECTION_STRING"],
            "container_id": dotenv["AZURE_BLOB_CONTAINER_ID"],
        }
        if not auth_data["connection_str"]:
            raise ValueError("connection string empty")
        if not auth_data["container_id"]:
            raise ValueError("container id empty")
    except (FileNotFoundError, ValueError, KeyError) as e:
        logger.exception("Failed to extract connection string from .env: %s", e)
        raise
    except Exception as e:
        logger.exception("Failed to load .env: %s", e)
        raise
    else:
        logger.info("Connection string loaded")
    return auth_data


def get_container_client(auth_data: dict) -> ContainerClient:
    connection_string = auth_data["connection_str"]
    container_id = auth_data["container_id"]
    try:
        logger.info("Creating blob container object: %s", container_id)
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
def upload_files_to_container(
    container_client: ContainerClient, file_paths: list
) -> list:
    uploaded_files = []
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
            uploaded_files.append(file_name)
            logger.debug("Blob properties: %s", blob_client.get_blob_properties())
            logger.info("File uploaded to blob container")
    return uploaded_files


def upload_blob(ti=None):
    setup_logger()
    logger.info("--- Blob runner started ---")

    auth_data = get_dotenv_auth_data()
    container_client = get_container_client(auth_data)
    file_paths = get_files_paths_to_upload("data/processed")
    uploaded_files = upload_files_to_container(container_client, file_paths)
    if ti:
        ti.xcom_push(key="uploaded_files", value=uploaded_files)

    logger.info("--- Blob runner ended ---")


if __name__ == "__main__":
    upload_blob(None)
