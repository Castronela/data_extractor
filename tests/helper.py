from typing import Callable
from logging import Logger


def check_for_raised_exception(
    exception: Exception,
    description: str,
    test_logger: Logger,
    func: Callable,
    *args,
    **kwargs,
):
    raised = False
    exc_type = "None"
    try:
        func(*args, **kwargs)
    except exception:
        raised = True
    except Exception as e:
        exc_type = type(e)
    if raised:
        test_logger.info("PASSED: %s", description)
    else:
        test_logger.error(
            "FAILED: %s: Expected %s, instead got %s", description, exception, exc_type
        )
        assert False, f"Expected exception {exception}, instead got {exc_type}"
