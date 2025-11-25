import logging
import time  # Timing
from datetime import datetime
from pathlib import Path

from src.sync.backup import cleanup_old_backups
from src.sync.core import (
    apply_incremental_update,
    process_full_backup,
)
from src.sync.file_discovery import (
    get_full_files,
    get_incremental_files,
)
from src.sync.reporting import save_combined_report
from src.sync.sync_state import load_state, save_state
from src.utils.file_operations import cleanup_old_archives


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
    logger: logging.Logger | None = None,
) -> bool:
    """
    Main function to check for updates and rebuild parquet file if needed
    Automatically called at program start

    NEW: Now supports daily incremental + weekly full backup pattern

    Args:
        config: Configuration dictionary
        paths: Paths dictionary from get_config_paths()
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

    # === NEW MODE: Auto-detect full backup + daily incrementals ===
    logger.info('=== Auto-Detection Mode: Full Backup + Daily Incrementals ===')

    # Check for new full backup files (e.g. 8 plant files)
    full_backup_files = get_full_files(main_folder, config)

    # Detect if there are new full backup files
    # Logic: Any full backup file in the folder is considered "new" and triggers a rebuild
    # because processed files are archived.

    if full_backup_files:
        logger.info(f'✓ Found {len(full_backup_files)} full backup file(s)')
        logger.info('=== Processing New Full Backup ===')
        total_start_time = time.time()

        try:
            final_df, validation_results = process_full_backup(full_backup_files, config, paths)
            total_processing_time = time.time() - total_start_time

            change_results = {
                'has_changes': False,
                'changes_summary': None,
                'changes_df': None,
                'new_rows_df': None,
                'updated_rows_df': None,
            }

            report_files = save_combined_report(validation_results, change_results, total_processing_time, audit_folder)

            # Update state
            state.update(
                {
                    'last_update': datetime.now().isoformat(),
                    'last_full_backup': datetime.now().isoformat(),
                    'last_full_backup_files': [str(f) for f in full_backup_files],
                    'applied_incrementals': [],
                    'row_count': len(final_df),
                    'column_count': len(final_df.columns),
                    'last_validation_summary': {
                        'has_issues': validation_results.get('has_issues'),
                        'contracts_with_multiple_vendors': len(validation_results.get('contracts_with_multiple_vendors', [])),
                        'blank_vendor_catalogue_count': validation_results.get('blank_vendor_catalogue_count', 0),
                        'inconsistent_vendor_catalogue_count': validation_results.get('inconsistent_vendor_catalogue_count', 0),
                    },
                }
            )
            save_state(state_file, state)

            retention_days = config.get('update_settings', {}).get('backup_retention_days', 7)
            cleanup_old_backups(backup_folder, retention_days)

            # Archive processed files
            if archive_folder:
                should_archive = config.get('file_patterns', {}).get('0031_full', {}).get('archive_after_processing', True)
                if should_archive:
                    logger.info('Archiving processed full backup files...')
                    for file_path in full_backup_files:
                        from src.utils.file_operations import archive_file
                        archive_file(file_path, archive_folder)

            logger.info('=== Full Backup Completed Successfully ===')
            logger.info(f'Rows: {len(final_df):,}')
            logger.info(f'Columns: {len(final_df.columns)}')
            logger.info('')
            logger.info('Reports saved:')
            if report_files.get('markdown_file'):
                logger.info(f'  - Markdown: {Path(report_files["markdown_file"]).name}')
            if report_files.get('excel_file'):
                logger.info(f'  - Excel: {Path(report_files["excel_file"]).name}')

            return True

        except Exception as e:
            logger.error(f'Error during full backup processing: {e}')
            import traceback
            logger.debug(traceback.format_exc())
            raise

    # Check for incremental files
    all_incremental_files = get_incremental_files(main_folder, config)

    # Logic: Any incremental file in the folder is considered "new" and triggers update
    # because processed files are archived.

    if all_incremental_files:
        logger.info(f'✓ Found {len(all_incremental_files)} incremental file(s)')

        # Sort by filename (assumes date in filename for chronological order)
        all_incremental_files.sort()

        # Check max incrementals per run
        max_incrementals = config.get('processing_schedule', {}).get('max_incrementals_per_run', 10)
        if len(all_incremental_files) > max_incrementals:
            logger.warning(f'Warning: {len(all_incremental_files)} files found, limiting to {max_incrementals} per run')
            all_incremental_files = all_incremental_files[:max_incrementals]

        total_start_time = time.time()

        try:
            # Process multiple incrementals in ONE efficient batch operation
            final_df, validation_results, change_results = apply_incremental_update(
                db_file,
                all_incremental_files,
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
            new_applied = set(state.get('applied_incrementals', [])) | {f.name for f in all_incremental_files}
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
            logger.info(f'Processed {len(all_incremental_files)} incremental file(s) in batch')
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

    # No weekly full or incrementals found
    logger.info('No new weekly full or incremental files detected')
    logger.info('Parquet file is up to date')
    return False
