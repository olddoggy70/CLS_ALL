"""
Generic file operations utilities.

Shared utilities for file archiving, cleanup, and date parsing
across all pipeline phases.
"""

import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path


def archive_file(file_path: Path, archive_folder: Path, logger: logging.Logger | None = None) -> Path:
    """
    Move file to archive folder.

    Args:
        file_path: File to archive
        archive_folder: Destination archive folder
        logger: Logger instance

    Returns:
        Path to archived file
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.utils.file_operations')

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
    Remove archived files older than retention period.

    Args:
        archive_folder: Folder containing archived files
        retention_days: Number of days to retain archived files
        logger: Logger instance
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.utils.file_operations')

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


def parse_date_from_filename(filename: str, date_format: str) -> datetime:
    """
    Extract date from filename using configured format.

    Args:
        filename: Filename to parse
        date_format: strptime format string

    Returns:
        datetime object or None if parsing fails
    """
    try:
        return datetime.strptime(filename, date_format)
    except ValueError:
        return None
