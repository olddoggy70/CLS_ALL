"""
Phase 0: Database Sync
Sync 0031 database from incremental backups

Public API for database synchronization operations.
All configuration and paths must be provided by the calling application.
"""

import logging
from pathlib import Path

from .sync_core.orchestrator import update_parquet_if_needed
from .sync_core.state import get_update_status
from .sync_core.state import print_status as print_status_internal


def auto_check_and_update(config: dict, paths: dict, logger: logging.Logger | None = None) -> bool:
    """
    Automatically check and update database if needed.
    This should be called at the start of your main program.

    Args:
        config: Configuration dict loaded by main.py
        paths: Paths dict prepared by main.py
        logger: Logger instance for output

    Returns:
        True if update was performed, False otherwise
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    return update_parquet_if_needed(config, paths, force_rebuild=False, logger=logger)


def daily_update(config: dict, paths: dict, logger: logging.Logger | None = None) -> bool:
    """
    Run daily update - checks for changes and updates if needed.

    Args:
        config: Configuration dict
        paths: Paths dict
        logger: Logger instance for output

    Returns:
        True if update was performed, False otherwise
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    return update_parquet_if_needed(config, paths, force_rebuild=False, logger=logger)


def force_update(config: dict, paths: dict, logger: logging.Logger | None = None) -> bool:
    """
    Force full rebuild regardless of changes.

    Args:
        config: Configuration dict
        paths: Paths dict
        logger: Logger instance for output

    Returns:
        True if update was performed, False otherwise
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    return update_parquet_if_needed(config, paths, force_rebuild=True, logger=logger)


def apply_incremental(config: dict, paths: dict, file_path: str, logger: logging.Logger | None = None) -> bool:
    """
    Apply single incremental file (legacy mode).

    Args:
        config: Configuration dict
        paths: Paths dict
        file_path: Path to incremental Excel file
        logger: Logger instance for output

    Returns:
        True if update was performed, False otherwise
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    return update_parquet_if_needed(config, paths, force_rebuild=False, incremental_file=Path(file_path), logger=logger)


def get_status(config: dict, paths: dict) -> dict:
    """
    Get current status of database and updates.

    Args:
        config: Configuration dict
        paths: Paths dict

    Returns:
        Dictionary with status information
    """
    return get_update_status(config, paths)


def print_status(config: dict, paths: dict, logger: logging.Logger | None = None):
    """
    Print current status in readable format.

    Args:
        config: Configuration dict
        paths: Paths dict
        logger: Logger instance for output
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    print_status_internal(config, paths)


# ============================================================================
# Phase 0 Alias (for consistency with other phase modules)
# ============================================================================


def process_sync(config: dict, paths: dict, logger: logging.Logger | None = None) -> bool:
    """
    Phase 0: Sync 0031 database from incremental backups

    This is an alias for auto_check_and_update() to maintain naming
    consistency with other phase modules (process_integrate, process_classify, etc.)

    Args:
        config: Configuration dict
        paths: Paths dict
        logger: Logger instance for output

    Returns:
        True if sync succeeded (note: no updates needed is not a failure)
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    try:
        result = auto_check_and_update(config, paths, logger)

        if result:
            logger.info('✓ Phase 0 (Sync) completed successfully')
        else:
            logger.info('↔ Phase 0 (Sync): Database already up to date')

        return True  # Always return True (no updates needed is not a failure)

    except Exception as e:
        logger.error(f'Phase 0 (Sync) failed: {e}')
        raise


# ============================================================================
# Public API Exports
# ============================================================================

__all__ = [
    'apply_incremental',
    'auto_check_and_update',
    'daily_update',
    'force_update',
    'get_status',
    'print_status',
    'process_sync',  # Phase 0 alias
]
