# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""Logging utilities."""

import logging
from pathlib import Path


def setup_logger(log_file: Path, logger_name: str = "db_migration") -> logging.Logger:
    """
    Setup logger with file and console handlers.
    
    Args:
        log_file: Path to log file
        logger_name: Name for the logger
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_file.parent.mkdir(parents=True, exist_ok=True)

    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger

