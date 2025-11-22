import json
import logging
from datetime import datetime
from pathlib import Path


def load_state(state_file: Path) -> dict:
    """Load update state from JSON file"""
    if state_file.exists():
        with open(state_file) as f:
            return json.load(f)
    return {
        'last_update': None,
        'file_timestamps': {},
        'last_backup': None,
        'last_full_backup': None,
        'last_full_backup_file': None,
        'applied_incrementals': [],
    }


def save_state(state_file: Path, state: dict):
    """Save update state to JSON file"""
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)


def get_update_status(config: dict, paths: dict) -> dict:
    """Get current status of parquet file and updates"""
    state = load_state(paths['state_file'])
    db_file = paths['db_file_path']
    backup_folder = paths['backup_folder']
    archive_folder = paths['archive_folder']

    status = {
        'parquet_exists': db_file.exists(),
        'last_update': state.get('last_update'),
        'last_full_backup': state.get('last_full_backup'),
        'last_full_backup_file': state.get('last_full_backup_file'),
        'applied_incrementals_count': len(state.get('applied_incrementals', [])),
        'tracked_files': len(state.get('file_timestamps', {})),
        'row_count': state.get('row_count'),
        'column_count': state.get('column_count'),
    }

    if db_file.exists():
        status['parquet_size_mb'] = db_file.stat().st_size / (1024 * 1024)
        status['parquet_modified'] = datetime.fromtimestamp(db_file.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')

    if backup_folder.exists():
        backups = list(backup_folder.glob('*.parquet'))
        status['backup_count'] = len(backups)
        if backups:
            latest_backup = max(backups, key=lambda p: p.stat().st_mtime)
            status['latest_backup'] = latest_backup.name
    else:
        status['backup_count'] = 0

    if archive_folder.exists():
        archives = list(archive_folder.glob('*.xlsx'))
        status['archive_count'] = len(archives)
    else:
        status['archive_count'] = 0

    return status


def print_status(config: dict, paths: dict, logger: logging.Logger | None = None):
    """Print current status in readable format"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.state')

    logger.info('=== Parquet File Status ===')
    status = get_update_status(config, paths)

    for key, value in status.items():
        logger.info(f'{key.replace("_", " ").title()}: {value}')

    # Show applied incrementals
    state = load_state(paths['state_file'])
    applied = state.get('applied_incrementals', [])
    if applied:
        logger.info(f'Applied Incremental Files ({len(applied)}):')
        for inc_file in applied[-5:]:  # Show last 5
            logger.info(f'  - {inc_file}')
        if len(applied) > 5:
            logger.info(f'  ... and {len(applied) - 5} more')
