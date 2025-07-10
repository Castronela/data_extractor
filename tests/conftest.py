import pytest
import logging
import json

logger = logging.getLogger("conftest")


def setup_logging():
    config_file = "config/logging.json"
    try:
        with open(config_file, encoding="utf-8") as file:
            config = json.load(file)
        logging.config.dictConfig(config)
    except Exception as e:
        logging.exception("Logging setup failed: %s", e)
        raise


@pytest.fixture(scope="session", autouse=True)
def test_session_header():
    separator = "=" * 80

    logger.info("%s", separator)
    logger.info("TEST SESSION STARTED: %s")
    logger.info("%s", separator)

    yield

    logger.info("%s", separator)
    logger.info("TEST SESSION ENDED: %s")
    logger.info("%s", separator)
