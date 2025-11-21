import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# class TimerAdapter(logging.LoggerAdapter):
#     def __init__(self, logger):
#         super().__init__(logger, {})
#         self.last_time = time.time()

#     def process(self, msg, kwargs):
#         now = time.time()
#         elapsed_ms = int((now - self.last_time) * 1000)
#         self.last_time = now
#         return f'{msg} [{elapsed_ms}ms]', kwargs


class TimingFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.last_time = time.time()
    def filter(self, record):
        # Only calculate if not already calculated for this record
        # (In case multiple handlers use the same filter instance or record is reused)
        if not hasattr(record, 'elapsed_ms'):
            now = time.time()
            record.elapsed_ms = int((now - self.last_time) * 1000)
            self.last_time = now
        return True

def setup_logging(log_folder: Path, console_level: str = 'INFO', file_level: str = 'DEBUG', enable_timing: bool = False):
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
    # file_formatter = logging.Formatter('%(asctime)s - %(name)s - [%(module)s] - %(levelname)s - %(message)s')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s -[%(name)s] - %(module)s - %(funcName)s - %(message)s [%(elapsed_ms)sms]')
    file_handler.setFormatter(file_formatter)

    # Console handler (brief, no module names)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level))
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    if enable_timing:
        timing_filter = TimingFilter()
        file_handler.addFilter(timing_filter)
        console_handler.addFilter(timing_filter)

    return logger, log_file
