"""Library with helpful methods for making changes to the code ocean index."""
__version__ = "0.1.3"

# Set default logging handler to avoid "No handler found" warnings.
# Borrowing logging setup from urllib3: https://urllib3.readthedocs.io/
import logging
from logging import NullHandler

logging.getLogger(__name__).addHandler(NullHandler())


def add_stderr_logger(level: int = logging.DEBUG) -> logging.StreamHandler:
    """
    Helper for quickly adding a StreamHandler to the logger. Useful for
    debugging.

    Returns the handler after adding it.
    """
    # This method needs to be in this __init__.py to get the __name__ correct
    # even if this packaged is vendored within another package.
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.debug("Added a stderr logging handler to logger: %s", __name__)
    return handler


# ... Clean up.
del NullHandler
