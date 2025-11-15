import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

# Import from state module
from .state import load_state


def get_excel_files(main_folder: Path, logger: logging.Logger | None = None) -> list[Path]:
    """Get all Excel files from main folder"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.files')

    files = list(main_folder.rglob('*.xlsx'))
    logger.debug(f'Found {len(files)} Excel file(s) in {main_folder}')
    return files


def get_incremental_files(main_folder: Path, config: dict, logger: logging.Logger | None = None) -> list[Path]:
    """Get all incremental files matching the pattern"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.files')

    pattern = config.get('file_patterns', {}).get('daily_incremental', {}).get('pattern', 'incremental_*.xlsx')
    files = sorted(main_folder.glob(pattern))
    logger.debug(f'Found {len(files)} incremental file(s) matching pattern: {pattern}')
    return files


def get_weekly_full_files(main_folder: Path, config: dict, logger: logging.Logger | None = None) -> list[Path]:
    """Get all weekly full files matching the pattern"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.files')

    pattern = config.get('file_patterns', {}).get('weekly_full', {}).get('pattern', 'full_week*.xlsx')
    files = sorted(main_folder.glob(pattern))
    logger.debug(f'Found {len(files)} weekly full file(s) matching pattern: {pattern}')
    return files


def parse_date_from_filename(filename: str, date_format: str) -> datetime:
    """Extract date from filename using configured format"""
    try:
        return datetime.strptime(filename, date_format)
    except ValueError:
        return None


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
    date_format = config.get('file_patterns', {}).get(file_type, {}).get('date_format')
    if not date_format:
        return None

    return parse_date_from_filename(file_path.name, date_format)


def check_for_changes(main_folder: Path, state_file: Path, logger: logging.Logger | None = None) -> tuple[bool, dict]:
    """Check if any Excel files have changed using file modification timestamps"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.files')

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


def archive_file(file_path: Path, archive_folder: Path, logger: logging.Logger | None = None) -> Path:
    """
    Move file to archive folder

    Args:
        file_path: File to archive
        archive_folder: Destination archive folder
        logger: Logger instance

    Returns:
        Path to archived file
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.files')

    archive_folder.mkdir(parents=True, exist_ok=True)
    archive_path = archive_folder / file_path.name

    # If file already exists in archive, add timestamp to avoid collision
    if archive_path.exists():
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_path = archive_folder / f'{file_path.stem}_{timestamp}{file_path.suffix}'

    shutil.move(str(file_path), str(archive_path))
    logger.debug(f'✓ Archived: {file_path.name} → archive/')

    return archive_path


def cleanup_old_archives(archive_folder: Path, retention_days: int = 90, logger: logging.Logger | None = None):
    """
    Remove archived incremental files older than retention period

    Args:
        archive_folder: Folder containing archived files
        retention_days: Number of days to retain archived files
        logger: Logger instance
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.files')

    if not archive_folder.exists():
        return

    cutoff_date = datetime.now() - timedelta(days=retention_days)
    removed_count = 0

    for archived_file in archive_folder.glob('*.xlsx'):
        file_mtime = datetime.fromtimestamp(archived_file.stat().st_mtime)
        if file_mtime < cutoff_date:
            logger.debug(f'Removing old archive: {archived_file.name}')
            archived_file.unlink()
            removed_count += 1

    if removed_count > 0:
        logger.debug(f'Removed {removed_count} old archived file(s)')
    else:
        logger.debug(f'No archives older than {retention_days} days found')


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
        logger = logging.getLogger('data_pipeline.sync.files')

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
