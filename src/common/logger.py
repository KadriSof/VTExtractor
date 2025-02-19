import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime

from pythonjsonlogger.json import JsonFormatter


LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

LOG_FILE = LOGS_DIR / f"vt_extractor_pipeline_{datetime.now().strftime(format='%Y%m%d-%H%M%S')}.log"


def setup_logger(name: str = "vt_extractor_pipeline", log_level: str = "INFO") -> logging.Logger:
    """
    Sets up and configure a logger.

    :param name: Name of the logger. Defaults to "vt_extractor_pipeline".
    :param log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to INFO.
    :return: logging.Logger: Configured logger instance.
    """
    vt_logger = logging.getLogger(name=name)
    vt_logger.setLevel(level=log_level)

    formatter = JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(fmt=formatter)

    log_file_handler = RotatingFileHandler(filename=LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5)
    log_file_handler.setFormatter(fmt=formatter)

    vt_logger.addHandler(hdlr=console_handler)
    vt_logger.addHandler(hdlr=log_file_handler)

    return vt_logger


logger = setup_logger()
