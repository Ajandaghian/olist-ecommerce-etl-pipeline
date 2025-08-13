import logging
import logging.config
from dotenv import load_dotenv
import os

load_dotenv()

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": """ ======log======= [%(levelname)s] %(name)s (%(funcName)s) (%(lineno)d): (%(asctime)s) ====== \n =====> %(message)s   \n """,
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": os.getenv("LOG_LEVEL", "DEBUG"),
        }
    },
    "root": {
        "handlers": ["console"],
        "level": os.getenv("LOG_LEVEL", "DEBUG"),
    }
}

logging.config.dictConfig(LOGGING_CONFIG)


def get_logger(name=None):
    """

    Get a logger with the specified name.
    If no name is provided, uses the caller's module name.
    """
    return logging.getLogger(name)


if __name__ == "__main__":
    logger = get_logger(__name__)
    logger.info("Logging configuration loaded successfully.")
