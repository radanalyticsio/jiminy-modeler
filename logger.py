"""Defining logging for the jiminy-modeller."""
import logging


def get_logger():
    logger = logging.getLogger("jiminy-modeller")
    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s-%(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.setLevel(logging.DEBUG)

    return logger
