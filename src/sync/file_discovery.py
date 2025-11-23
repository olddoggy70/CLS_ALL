import logging
from datetime import datetime
from pathlib import Path

# Import from sync_state module
from .sync_state import load_state


def get_excel_files(main_folder: Path, logger: logging.Logger | None = None) -> list[Path]:
    """Get all Excel files from main folder"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

            removed_count += 1

    if removed_count > 0:
        logger.debug(f'Removed {removed_count} old full backup file(s)')
        logger.debug(f'✓ Keeping latest full backup: {current_full_file.name}')
