import logging
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(log_folder: Path, console_level: str = 'INFO', file_level: str = 'DEBUG'):
    """
    Setup dual logging: console (brief) + file (detailed)

    Console levels:
    - INFO: High-level progress (default)
    - WARNING: Only warnings/errors
    - DEBUG: Everything (verbose)

    File always gets DEBUG level with module names
    """

    log_folder.mkdir(parents=True, exist_ok=True)

    # Create daily log file (one file per day, all commands append to it)
    date_str = datetime.now().strftime('%Y%m%d')
    log_file = log_folder / f'pipeline_{date_str}.log'

    # Create logger
    logger = logging.getLogger('data_pipeline')
    logger.setLevel(logging.DEBUG)

    # Clear any existing handlers to avoid duplicates
    logger.handlers.clear()

    # File handler (detailed with module names)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(getattr(logging, file_level))
    # Format: timestamp - module name - [source file] - level - message
    file_formatter = logging.Formatter('%(asctime)s - %(name)-30s - [%(module)s] - %(levelname)-8s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Console handler (brief, no module names)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level))
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger, log_file
