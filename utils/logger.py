import logging

from utils.config import config


def get_logger(name, log_level: str = config["logging"]["level"]):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] {%(filename)20s:%(lineno)-4s} %(levelname)5s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(log_level)
    return logger
