import pytest
import logging
from src.helper import setup_logger

logger = logging.getLogger("conftest")
setup_logger()


@pytest.fixture(scope="session", autouse=True)
def test_session_header():
    separator = "=" * 80

    logger.info("%s", separator)
    logger.info("TEST SESSION STARTED")
    logger.info("%s", separator)

    yield

    logger.info("%s", separator)
    logger.info("TEST SESSION ENDED")
    logger.info("%s", separator)
