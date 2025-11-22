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
    """Get all incremental files matching the pattern"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    pattern = config.get('file_patterns', {}).get('daily_incremental', {}).get('pattern', 'incremental_*.xlsx')
    files = sorted(main_folder.glob(pattern))
    logger.debug(f'Found {len(files)} incremental file(s) matching pattern: {pattern}')
    return files


def get_weekly_full_files(main_folder: Path, config: dict, logger: logging.Logger | None = None) -> list[Path]:
    """Get all weekly full files matching the pattern"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    pattern = config.get('file_patterns', {}).get('weekly_full', {}).get('pattern', 'full_week*.xlsx')
    files = sorted(main_folder.glob(pattern))
    logger.debug(f'Found {len(files)} weekly full file(s) matching pattern: {pattern}')
    return files


def get_file_date(file_path: Path, config: dict, file_type: str) -> datetime:
    """
    Extract date from filename based on configured date format

    Args:
        file_path: Path to the file
        config: Configuration dictionary
        file_type: Either 'daily_incremental' or 'weekly_full'

    Returns:
        datetime object or None if parsing fails
    """
    from ..utils.file_operations import parse_date_from_filename
    
    date_format = config.get('file_patterns', {}).get(file_type, {}).get('date_format')
    if not date_format:
        return None

    return parse_date_from_filename(file_path.name, date_format)


def check_for_changes(main_folder: Path, state_file: Path, logger: logging.Logger | None = None) -> tuple[bool, dict]:
    """Check if any Excel files have changed using file modification timestamps"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    excel_files = get_excel_files(main_folder)
    if not excel_files:
        logger.debug('No Excel files found')
        return False, {}

    state = load_state(state_file)
    old_timestamps = state.get('file_timestamps', {})
    current_timestamps = {}

    has_changes = False
    changed_files = []

    for file_path in excel_files:
        mtime = file_path.stat().st_mtime
        current_timestamps[str(file_path)] = mtime

        if str(file_path) not in old_timestamps:
            logger.debug(f'New file detected: {file_path.name}')
            has_changes = True
            changed_files.append(file_path.name)
        elif old_timestamps[str(file_path)] != mtime:
            logger.debug(f'Change detected: {file_path.name}')
            has_changes = True
            changed_files.append(file_path.name)

    # Check for deleted files
    for old_file in old_timestamps:
        if old_file not in current_timestamps:
            logger.debug(f'File deleted: {Path(old_file).name}')
            has_changes = True

    if has_changes:
        logger.debug(f'Total files with changes: {len(changed_files)}')
    else:
        logger.debug('No changes detected in Excel files')

    return has_changes, current_timestamps


def cleanup_old_full_backups(reports_folder: Path, current_full_file: Path, config: dict, logger: logging.Logger | None = None):
    """
    Remove old weekly full backup files, keeping only the current one

    Args:
        reports_folder: Folder containing full backup files
        current_full_file: The current/latest full backup file to keep
        config: Configuration dictionary
        logger: Logger instance
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    pattern = config.get('file_patterns', {}).get('weekly_full', {}).get('pattern', 'full_week*.xlsx')
    removed_count = 0

    for old_full in reports_folder.glob(pattern):
        if old_full != current_full_file:
            logger.debug(f'✓ Removing old full backup: {old_full.name}')
            old_full.unlink()
            removed_count += 1

    if removed_count > 0:
        logger.debug(f'Removed {removed_count} old full backup file(s)')
        logger.debug(f'✓ Keeping latest full backup: {current_full_file.name}')
