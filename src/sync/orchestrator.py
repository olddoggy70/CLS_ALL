import logging
import shutil  # Backup restoration
import time  # Timing
from datetime import datetime
from pathlib import Path

import polars as pl  # Read parquet files

from src.sync.backup import cleanup_old_backups, create_backup
from src.utils.file_operations import cleanup_old_archives
from src.sync.file_discovery import (
    check_for_changes,
    get_excel_files,
    get_incremental_files,
    get_weekly_full_files,
)
from src.sync.core import (
    apply_incremental_update,
    process_weekly_full_backup,
    rebuild_parquet,
)
from src.sync.transformation import apply_categorical_types
from src.sync.quality import track_row_changes, print_change_summary
from src.sync.reporting import save_combined_report
from src.sync.sync_state import load_state, save_state


def _clean_change_summary_for_state(change_summary: dict | None) -> dict | None:
    """
    Remove non-serializable DataFrames from change summary before saving to state JSON.
    """
    if not change_summary:
        return None

    # Create a copy to avoid modifying the original
    cleaned = change_summary.copy()

    # Remove date_breakdown DataFrames from per_file_summary
    if 'per_file_summary' in cleaned:
        cleaned_per_file = []
        for file_info in cleaned['per_file_summary']:
            file_info_copy = file_info.copy()
            # Remove the DataFrame object
            file_info_copy.pop('date_breakdown', None)
            cleaned_per_file.append(file_info_copy)
        cleaned['per_file_summary'] = cleaned_per_file

    return cleaned


def update_parquet_if_needed(
    config: dict,
    paths: dict,
    force_rebuild: bool = False,
    incremental_file: Path | None = None,
    logger: logging.Logger | None = None,
) -> bool:
    """
    Main function to check for updates and rebuild parquet file if needed
    Automatically called at program start

    NEW: Now supports daily incremental + weekly full backup pattern

    Args:
        config: Configuration dictionary
        paths: Paths dictionary from get_config_paths()
        force_rebuild: If True, rebuild regardless of changes
        incremental_file: If provided, apply incremental update from this Excel file (legacy mode)
        logger: Logger instance for output

    Returns:
        True if update was performed, False otherwise
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.orchestrator')

    logger.info('=== Checking Parquet File Status ===')
    logger.info(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    state_file = paths['state_file']
    main_folder = paths['main_folder']
    db_file = paths['db_file_path']
    backup_folder = paths['backup_folder']
    audit_folder = paths['audit_folder']
    archive_folder = paths['archive_folder']
    db_folder = paths['db_folder']

    # Ensure folders exist
    db_folder.mkdir(exist_ok=True)
    audit_folder.mkdir(exist_ok=True)
    archive_folder.mkdir(parents=True, exist_ok=True)

    # Load current state
    state = load_state(state_file)

    # === LEGACY MODE: Single incremental file provided ===
    if incremental_file:
        if not incremental_file.exists():
            logger.error(f'Incremental file not found: {incremental_file}')
            return False

        logger.info('=== Legacy Incremental Update Mode ===')
        total_start_time = time.time()

        try:
            updated_df, validation_results, change_results = apply_incremental_update(
                db_file,
                incremental_file,
                config,
                backup_folder,
                audit_folder,
                blank_vpn_permitted_file=paths.get('blank_vpn_permitted_file'),
                logger=logger,
            )

            # Calculate processing time
            total_processing_time = time.time() - total_start_time

            # Save combined report
            report_files = save_combined_report(validation_results, change_results, total_processing_time, audit_folder)

            # Clean change_summary before saving (remove non-serializable DataFrames)
            change_summary_for_state = _clean_change_summary_for_state(change_results.get('changes_summary'))

            # Update state
            state.update(
                {
                    'last_update': datetime.now().isoformat(),
                    'last_incremental_file': str(incremental_file),
                    'row_count': len(updated_df),
                    'column_count': len(updated_df.columns),
                    'last_change_summary': change_summary_for_state,
                    'last_validation_summary': {
                        'has_issues': validation_results.get('has_issues'),
                        'contracts_with_multiple_vendors': len(validation_results.get('contracts_with_multiple_vendors', [])),
                        'blank_vendor_catalogue_count': validation_results.get('blank_vendor_catalogue_count', 0),
                        'inconsistent_vendor_catalogue_count': validation_results.get('inconsistent_vendor_catalogue_count', 0),
                    },
                }
            )
            save_state(state_file, state)

            # Cleanup old backups
            retention_days = config.get('update_settings', {}).get('backup_retention_days', 7)
            cleanup_old_backups(backup_folder, retention_days)

            logger.info('=== Incremental Update Completed Successfully ===')
            logger.info(f'Rows: {len(updated_df):,}')
            logger.info(f'Columns: {len(updated_df.columns)}')
            logger.info('')
            logger.info('Reports saved:')
            if report_files.get('markdown_file'):
                logger.info(f'  - Markdown: {Path(report_files["markdown_file"]).name}')
            if report_files.get('excel_file'):
                logger.info(f'  - Excel: {Path(report_files["excel_file"]).name}')

            return True

    all_incremental_files = get_incremental_files(main_folder, config)
    applied_incrementals = set(state.get('applied_incrementals', []))

    # Filter to only unapplied incremental files (use filename only for consistency)
    unapplied_incrementals = [f for f in all_incremental_files if f.name not in applied_incrementals]

    if unapplied_incrementals:
        logger.info(f'✓ Found {len(unapplied_incrementals)} unapplied incremental file(s)')

        # Sort by filename (assumes date in filename for chronological order)
        unapplied_incrementals.sort()

        # Check max incrementals per run
        max_incrementals = config.get('processing_schedule', {}).get('max_incrementals_per_run', 10)
        if len(unapplied_incrementals) > max_incrementals:
            logger.warning(f'Warning: {len(unapplied_incrementals)} files found, limiting to {max_incrementals} per run')
            unapplied_incrementals = unapplied_incrementals[:max_incrementals]

        total_start_time = time.time()

        try:
            # Process multiple incrementals in ONE efficient batch operation
            final_df, validation_results, change_results = apply_incremental_update(
                db_file,
                unapplied_incrementals,
                config,
                backup_folder,
                audit_folder,
                archive_folder,
                blank_vpn_permitted_file=paths.get('blank_vpn_permitted_file'),
            )

            # Calculate processing time
            total_processing_time = time.time() - total_start_time

            # Save combined report
            report_files = save_combined_report(validation_results, change_results, total_processing_time, audit_folder)

            # Clean change_summary before saving (remove non-serializable DataFrames)
            change_summary_for_state = _clean_change_summary_for_state(change_results.get('changes_summary'))

            # Update state - add processed files to applied list (use filename only)
            new_applied = applied_incrementals | {f.name for f in unapplied_incrementals}
            state.update(
                {
                    'last_update': datetime.now().isoformat(),
                    'applied_incrementals': list(new_applied),
                    'row_count': len(final_df),
                    'column_count': len(final_df.columns),
                    'last_change_summary': change_summary_for_state,
                    'last_validation_summary': {
                        'has_issues': validation_results.get('has_issues'),
                        'contracts_with_multiple_vendors': len(validation_results.get('contracts_with_multiple_vendors', [])),
                        'blank_vendor_catalogue_count': validation_results.get('blank_vendor_catalogue_count', 0),
                        'inconsistent_vendor_catalogue_count': validation_results.get('inconsistent_vendor_catalogue_count', 0),
                    },
                }
            )
            save_state(state_file, state)

            # Cleanup old backups and archives
            retention_days = config.get('update_settings', {}).get('backup_retention_days', 7)
            cleanup_old_backups(backup_folder, retention_days)

            archive_retention = config.get('archive_settings', {}).get('retention_days', 90)
            cleanup_old_archives(archive_folder, archive_retention)

            logger.info('=== Incremental Updates Completed Successfully ===')
            logger.info(f'Processed {len(unapplied_incrementals)} incremental file(s) in batch')
            logger.info(f'Final rows: {len(final_df):,}')
            logger.info(f'Columns: {len(final_df.columns)}')
            logger.info('')
            logger.info('Reports saved:')
            if report_files.get('markdown_file'):
                logger.info(f'  - Markdown: {Path(report_files["markdown_file"]).name}')
            if report_files.get('excel_file'):
                logger.info(f'  - Excel: {Path(report_files["excel_file"]).name}')

            return True

        except Exception as e:
            logger.error(f'Error during incremental updates: {e}')
            import traceback
            logger.debug(traceback.format_exc())
            raise

    # No weekly full or incrementals found - fall back to original logic
    logger.info('No new weekly full or incremental files detected')
    logger.debug('Checking for general file changes...')

    if not force_rebuild:
        has_changes, current_timestamps = check_for_changes(main_folder, state_file)
        if not has_changes:
            logger.info('Parquet file is up to date - no rebuild needed')
            return False
    else:
        logger.info('Force rebuild requested')
        current_timestamps = {}
        for file_path in get_excel_files(main_folder):
            current_timestamps[str(file_path)] = file_path.stat().st_mtime

    logger.info('=== Starting Parquet Rebuild ===')

    # Load previous dataframe for change tracking
    previous_df = None
    if db_file.exists():
        try:
            logger.debug('Loading previous parquet for comparison...')
            previous_df = pl.read_parquet(db_file)
            # Apply categorical types for in-memory operations
            previous_df = apply_categorical_types(previous_df, config)
        except Exception as e:
            logger.warning(f'Could not load previous parquet for comparison: {e}')

    # Create backup of existing parquet file
    backup_path = create_backup(db_file, backup_folder)

    try:
        # Start total processing timer
        total_start_time = time.time()

        # Rebuild parquet file (returns validation results too)
        final_df, validation_results = rebuild_parquet(
            main_folder, db_file, config, blank_vpn_permitted_file=paths.get('blank_vpn_permitted_file')
        )

        # Track changes if previous data exists
        change_results = {
            'has_changes': False,
            'changes_summary': None,
            'changes_df': None,
            'new_rows_df': None,
            'updated_rows_df': None,
        }

        if previous_df is not None:
            change_results = track_row_changes(final_df, previous_df, audit_folder)
            print_change_summary(change_results)

        # Calculate total processing time
        total_processing_time = time.time() - total_start_time

        # Save combined report (validation + changes)
        report_files = save_combined_report(validation_results, change_results, total_processing_time, audit_folder)

        # Clean change_summary before saving (remove non-serializable DataFrames)
        change_summary_for_state = _clean_change_summary_for_state(change_results.get('changes_summary'))

        # Update state
        state = {
            'last_update': datetime.now().isoformat(),
            'file_timestamps': current_timestamps,
            'last_backup': str(backup_path) if backup_path else None,
            'row_count': len(final_df),
            'column_count': len(final_df.columns),
            'last_change_summary': change_summary_for_state,
            'last_validation_summary': {
                'has_issues': validation_results.get('has_issues'),
                'contracts_with_multiple_vendors': len(validation_results.get('contracts_with_multiple_vendors', [])),
                'blank_vendor_catalogue_count': validation_results.get('blank_vendor_catalogue_count', 0),
                'inconsistent_vendor_catalogue_count': validation_results.get('inconsistent_vendor_catalogue_count', 0),
            },
        }
        save_state(state_file, state)

        # Cleanup old backups
        retention_days = config.get('update_settings', {}).get('backup_retention_days', 7)
        cleanup_old_backups(backup_folder, retention_days)

        logger.info('=== Parquet Update Completed Successfully ===')
        logger.info(f'Rows: {len(final_df):,}')
        logger.info(f'Columns: {len(final_df.columns)}')
        if backup_path:
            logger.info(f'Backup: {backup_path.name}')
        logger.info('')
        logger.info('Reports saved:')
        if report_files.get('markdown_file'):
            logger.info(f'  - Markdown: {Path(report_files["markdown_file"]).name}')
        if report_files.get('excel_file'):
            logger.info(f'  - Excel: {Path(report_files["excel_file"]).name}')

        return True

    except Exception as e:
        logger.error(f'Error during update: {e}')
        import traceback
        logger.debug(traceback.format_exc())

        # Restore from backup if it exists
        if backup_path and backup_path.exists():
            logger.info('Attempting to restore from backup...')
            shutil.copy2(backup_path, db_file)
            logger.info('Backup restored successfully')

        raise
