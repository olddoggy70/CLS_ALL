import logging
from datetime import datetime
from pathlib import Path

# Import from sync_state module
from .sync_state import load_state


def get_excel_files(main_folder: Path, logger: logging.Logger | None = None) -> list[Path]:
    """Get all Excel files from main folder"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    files = list(main_folder.rglob('*.xlsx'))
    logger.debug(f'Found {len(files)} Excel file(s) in {main_folder}')
    return files


def get_incremental_files(main_folder: Path, config: dict, logger: logging.Logger | None = None) -> list[Path]:
    """
    Get all incremental files matching the pattern, EXCLUDING full files

    The incremental pattern (e.g. *.xlsx) is often broad and matches files too.
    We must explicitly exclude them to avoid double processing.
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    # Get incremental files
    inc_pattern = config.get('file_patterns', {}).get('0031_incremental', {}).get('pattern', 'incremental_*.xlsx')
    inc_files = set(main_folder.glob(inc_pattern))

    # Get full files to exclude
    full_pattern = config.get('file_patterns', {}).get('0031_full', {}).get('pattern', 'full_week*.xlsx')
    full_files = set(main_folder.glob(full_pattern))

    # Subtract full files from incremental files
    final_files = sorted(list(inc_files - full_files))

    logger.debug(f'Found {len(final_files)} incremental file(s) matching pattern: {inc_pattern} (excluded {len(full_files)} full backups)')
    return final_files


def get_full_files(main_folder: Path, config: dict, logger: logging.Logger | None = None) -> list[Path]:
    """Get all full files matching the pattern"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    pattern = config.get('file_patterns', {}).get('0031_full', {}).get('pattern', 'full_week*.xlsx')
    files = sorted(main_folder.glob(pattern))
    logger.debug(f'Found {len(files)} full file(s) matching pattern: {pattern}')
    return files


def get_file_date(file_path: Path, config: dict, file_type: str) -> datetime:
    """
    Extract date from filename based on configured date format

    Args:
        file_path: Path to the file
        config: Configuration dictionary
        file_type: Either '0031_incremental' or '0031_full'

    Returns:
        datetime object or None if parsing fails
    """
    from ..utils.file_operations import parse_date_from_filename

    date_format = config.get('file_patterns', {}).get(file_type, {}).get('date_format')
    if not date_format:
        return None

    return parse_date_from_filename(file_path.name, date_format)




def cleanup_old_full_backups(reports_folder: Path, current_full_file: Path, config: dict, logger: logging.Logger | None = None):
    """
    Remove old full backup files, keeping only the current one

    Args:
        reports_folder: Folder containing full backup files
        current_full_file: The current/latest full backup file to keep
        config: Configuration dictionary
        logger: Logger instance
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    pattern = config.get('file_patterns', {}).get('0031_full', {}).get('pattern', 'full_week*.xlsx')
    removed_count = 0

    for old_full in reports_folder.glob(pattern):
        if old_full != current_full_file:
            logger.debug(f'✓ Removing old full backup: {old_full.name}')
            old_full.unlink()
            removed_count += 1

    if removed_count > 0:
        logger.debug(f'Removed {removed_count} old full backup file(s)')
        logger.debug(f'✓ Keeping latest full backup: {current_full_file.name}')
