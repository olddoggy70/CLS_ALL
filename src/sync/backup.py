import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path


def create_backup(db_file: Path, backup_folder: Path, logger: logging.Logger | None = None) -> Path | None:
    """Create timestamped backup of current parquet file"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.backup')

    if not db_file.exists():
        logger.debug('No existing parquet file to backup')
        return None

    # Ensure backup folder exists
    backup_folder.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = f'{db_file.stem}_backup_{timestamp}.parquet'
    backup_path = backup_folder / backup_name

    logger.debug(f'Creating backup: {backup_name}')
    shutil.copy2(db_file, backup_path)
    logger.debug(f'Backup created: {backup_path}')

    return backup_path


def cleanup_old_backups(backup_folder: Path, retention_days: int = 7, logger: logging.Logger | None = None):
    """Remove backups older than retention period"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.backup')

    if not backup_folder.exists():
        return

    cutoff_date = datetime.now() - timedelta(days=retention_days)
    removed_count = 0

    for backup_file in backup_folder.glob('*.parquet'):
        file_mtime = datetime.fromtimestamp(backup_file.stat().st_mtime)
        if file_mtime < cutoff_date:
            logger.debug(f'Removing old backup: {backup_file.name}')
            backup_file.unlink()
            removed_count += 1

    if removed_count > 0:
        logger.debug(f'Removed {removed_count} old backup(s)')
    else:
        logger.debug(f'No backups older than {retention_days} days found')
