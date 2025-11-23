"""
Data processing operations - cleaning, incremental updates, and full rebuilds
Refactored to use specialized modules: ingest, transformation, merge
"""

import logging
import time
from pathlib import Path

import polars as pl

from ..constants import Columns0031

# Import from utils
from ..utils.file_operations import archive_file

# Import from other sync modules
from . import ingest, merge, transformation
from .backup import create_backup
from .quality import track_row_changes, validate_parquet_data


def apply_incremental_update(
    db_file: Path,
    incremental_files: Path | list[Path],
    config: dict,
    backup_folder: Path,
    audit_folder: Path,
    archive_folder: Path | None = None,
    blank_vpn_permitted_file: Path | None = None,
    logger: logging.Logger | None = None,
) -> tuple[pl.DataFrame, dict, dict]:
    """
    Apply incremental updates from Excel file(s) to existing parquet file

    Args:
        db_file: Path to existing parquet file
        incremental_files: Single Path or list of Paths for incremental Excel files
        config: Configuration dictionary
        backup_folder: Folder to save backup
        audit_folder: Folder to save audit reports
        archive_folder: Optional folder to archive processed files (batch mode only)
        blank_vpn_permitted_file: Path to permitted blank VPN file
        logger: Logger instance

    Returns:
        Tuple of (updated_df, validation_results, change_results)
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    # Normalize input: always list
    if isinstance(incremental_files, Path):
        incremental_files = [incremental_files]

    logger.info('=== Applying Incremental Updates ===')
    logger.info(f'Processing {len(incremental_files)} file(s)')

    # Load existing parquet file
    if not db_file.exists():
        raise ValueError(f'Parquet file not found: {db_file}')

    logger.info(f'Loading existing parquet: {db_file.name}')
    current_df = pl.read_parquet(db_file)
    initial_row_count = len(current_df)
    logger.debug(f'  Current rows: {initial_row_count:,}')

    # Create ONE backup before processing
    logger.info('Creating backup before processing...')
    create_backup(db_file, backup_folder)

    # Load ALL incremental files
    # We use ingest.process_excel_files which handles the list and concatenation
    # But we need to keep track of individual files for reporting if in batch mode
    # So we'll do a hybrid approach similar to original but using ingest helpers if possible
    # Actually, the original logic had specific per-file tracking. Let's preserve that structure
    # but use the new modules for the heavy lifting.

    infer_schema_length = config.get('processing_options', {}).get('infer_schema_length', 0)

    # We need to load files individually to track metadata for reporting
    logger.info(f'Loading {len(incremental_files)} incremental file(s)...')
    all_incremental_dfs = []
    file_metadata = []

    for idx, file_path in enumerate(incremental_files, 1):
        try:
            logger.info(f'  [{idx}/{len(incremental_files)}] Loading {file_path.name}...')

            # Use ingest module for reading (even single file)
            # process_excel_files returns a concatenated DF, so we pass a list of 1
            df = ingest.process_excel_files([file_path], infer_schema_length, logger)

            if df is not None:
                # Add source_file column for accurate tracking
                df = df.with_columns(pl.lit(file_path.name).alias('source_file'))

                logger.debug(f'      Rows: {len(df):,}')
                file_metadata.append({'index': idx, 'filename': file_path.name, 'row_count': len(df), 'dataframe': df})
                all_incremental_dfs.append(df)

        except Exception as e:
            logger.error(f'Error loading {file_path.name}: {e}')
            raise ValueError(f'Failed to load incremental file {file_path.name}: {e}')

    if not all_incremental_dfs:
        raise ValueError('No data loaded from incremental files')

    # Concatenate all incrementals into ONE combined DataFrame
    logger.info('Combining all incremental data...')
    combined_incremental_df = pl.concat(all_incremental_dfs, how='diagonal')
    total_incremental_rows = len(combined_incremental_df)
    logger.debug(f'  Total incremental rows: {total_incremental_rows:,}')

    # Clean and optimize ONCE on combined data
    logger.debug('Cleaning and optimizing combined data...')
    combined_incremental_df = transformation.clean_dataframe(combined_incremental_df, logger)
    data_config = config.get('data_processing', {})
    combined_incremental_df = transformation.convert_and_optimize_columns(combined_incremental_df, config, logger)

    # Apply filters (incremental only)
    combined_incremental_df = transformation.apply_filters(combined_incremental_df, data_config, logger)

    # Validate required unique key columns
    unique_keys = [
        Columns0031.PMM_ITEM_NUMBER,
        Columns0031.CORP_ACCT,
        Columns0031.VENDOR_CODE,
        Columns0031.ADD_COST_CENTRE,
        Columns0031.ADD_GL_ACCOUNT,
    ]
    if not all(col in current_df.columns for col in unique_keys):
        raise ValueError(f'Current parquet missing required columns: {unique_keys}')

    if not all(col in combined_incremental_df.columns for col in unique_keys):
        raise ValueError(f'Incremental files missing required columns: {unique_keys}')

    # Smart Sync: Filter out outdated rows BEFORE tracking changes
    # This ensures reporting only reflects rows that will actually be applied
    
    # We need merge keys for filtering
    combined_incremental_df = merge.prepare_merge_keys(combined_incremental_df, logger)
    
    # Calculate rows before filtering
    rows_before_filter = len(combined_incremental_df)
    
    # Apply filter
    combined_incremental_df = merge.filter_outdated_rows(current_df, combined_incremental_df, logger)
    
    # Calculate skipped rows
    skipped_rows = rows_before_filter - len(combined_incremental_df)
    
    # Track changes - different approach for batch vs single file
    # This logic is specific to reporting/audit, so we keep it here or move to reporting
    # For now, keeping it here as it orchestrates the flow
    # PER-FILE tracking
    logger.info('Tracking changes per file...')
    all_change_results = []

    # We no longer rely on slicing which is fragile if rows are filtered
    # We use the source_file column we added earlier

    for file_info in file_metadata:
        file_idx = file_info['index']
        filename = file_info['filename']

        # Filter by source_file to get exact rows for this file
        file_df = combined_incremental_df.filter(pl.col('source_file') == filename)
        
        # Calculate per-file dropped rows
        original_rows = file_info['row_count']
        final_rows = len(file_df)
        dropped_rows_file = original_rows - final_rows

        logger.debug(f'  [{file_idx}/{len(file_metadata)}] Tracking changes from {filename}...')

        file_change_results = track_row_changes(file_df, current_df, audit_folder, logger)
        file_change_results['source_file'] = filename
        file_change_results['file_index'] = file_idx
        file_change_results['original_rows'] = original_rows
        file_change_results['dropped_rows'] = dropped_rows_file

        all_change_results.append(file_change_results)

    # Analyze duplicates
    logger.info('Analyzing duplicate items across files...')

    # We need a temp key for duplicate analysis
    combined_with_keys = merge.prepare_merge_keys(combined_incremental_df, logger)
    # Rename _merge_key to _temp_key for the duplicate logic which expects _temp_key
    combined_with_keys = combined_with_keys.rename({'_merge_key': '_temp_key'})

    duplicate_cols = [
        pl.count().alias('occurrence_count'),
        pl.col(Columns0031.PMM_ITEM_NUMBER).first().alias(Columns0031.PMM_ITEM_NUMBER),
        pl.col(Columns0031.CORP_ACCT).first().alias(Columns0031.CORP_ACCT),
        pl.col(Columns0031.VENDOR_CODE).first().alias(Columns0031.VENDOR_CODE),
        pl.col(Columns0031.ADD_COST_CENTRE).first().alias(Columns0031.ADD_COST_CENTRE),
        pl.col(Columns0031.ADD_GL_ACCOUNT).first().alias(Columns0031.ADD_GL_ACCOUNT),
        pl.col(Columns0031.ITEM_UPDATE_DATE).alias('Update_Dates'),
    ]

    if 'Default UOM Price' in combined_incremental_df.columns:
        duplicate_cols.append(pl.col('Default UOM Price').alias('Prices'))

    duplicate_analysis = (
        combined_with_keys.group_by('_temp_key')
        .agg(duplicate_cols)
        .filter(pl.col('occurrence_count') > 1)
        .sort('occurrence_count', descending=True)
    )

    duplicate_count = len(duplicate_analysis)
    duplicates_analysis_df = None
    duplicates_full_df = None

    if duplicate_count > 0:
        logger.debug(f'  Found {duplicate_count:,} items updated across multiple files')

        duplicate_keys = set(duplicate_analysis.select('_temp_key').to_series().to_list())
        duplicates_full_df = (
            combined_with_keys.filter(pl.col('_temp_key').is_in(list(duplicate_keys)))
            .drop('_temp_key')
            .sort(
                [
                    Columns0031.PMM_ITEM_NUMBER,
                    Columns0031.CORP_ACCT,
                    Columns0031.VENDOR_CODE,
                    Columns0031.ADD_COST_CENTRE,
                    Columns0031.ADD_GL_ACCOUNT,
                    Columns0031.ITEM_UPDATE_DATE,
                ]
            )
        )

        duplicates_analysis_df = duplicate_analysis.drop('_temp_key')
    else:
        logger.debug('  No duplicate items found')

    # Aggregate all change results
    aggregated_summary = {
        'new_rows': sum((c.get('changes_summary') or {}).get('new_rows', 0) for c in all_change_results),
        'updated_rows': sum((c.get('changes_summary') or {}).get('updated_rows', 0) for c in all_change_results),
        'skipped_rows': skipped_rows,
        'files_processed': len(all_change_results),
        'per_file_summary': [
            {
                'file': c['source_file'],
                'file_index': c['file_index'],
                'original_rows': c['original_rows'],
                'dropped_rows': c['dropped_rows'],
                'new_rows': (c.get('changes_summary') or {}).get('new_rows', 0),
                'updated_rows': (c.get('changes_summary') or {}).get('updated_rows', 0),
                'latest_update_date': (c.get('changes_summary') or {}).get('latest_update_date', 'N/A'),
                'date_breakdown': c.get('date_breakdown'),
            }
            for c in all_change_results
        ],
    }

    # Combine DFs for reporting
    all_changes_dfs = [c['changes_df'] for c in all_change_results if c.get('changes_df') is not None]
    all_new_rows_dfs = [c['new_rows_df'] for c in all_change_results if c.get('new_rows_df') is not None]
    all_updated_rows_dfs = [c['updated_rows_df'] for c in all_change_results if c.get('updated_rows_df') is not None]

    change_results = {
        'has_changes': True,
        'changes_summary': aggregated_summary,
        'changes_df': pl.concat(all_changes_dfs, how='diagonal') if all_changes_dfs else None,
        'new_rows_df': pl.concat(all_new_rows_dfs, how='diagonal') if all_new_rows_dfs else None,
        'updated_rows_df': pl.concat(all_updated_rows_dfs, how='diagonal') if all_updated_rows_dfs else None,
        'duplicates_summary': {'duplicate_count': duplicate_count, 'total_rows_before_dedup': total_incremental_rows},
        'duplicates_analysis_df': duplicates_analysis_df,
        'duplicates_full_df': duplicates_full_df,
    }

    # --- MERGE LOGIC START ---
    # Use the new merge module for the core logic

    # Drop source_file column before merge if it exists
    if 'source_file' in combined_incremental_df.columns:
        combined_incremental_df = combined_incremental_df.drop('source_file')

    # 1. Deduplicate incremental data
    combined_incremental_df, _ = merge.deduplicate_data(combined_incremental_df, unique_keys, logger)

    # 2. Prepare merge keys
    logger.debug('Creating merge keys for database update...')
    current_df = merge.prepare_merge_keys(current_df, logger)
    # combined_incremental_df already has keys from Smart Sync step

    # 3. Identify changes
    update_keys, _ = merge.identify_changes(current_df, combined_incremental_df, logger)

    # 4. Check for duplicate keys in DB (safety check)
    merge.check_duplicate_keys(current_df, update_keys, logger)

    # 5. Apply merge
    updated_df = merge.merge_dataframes(current_df, combined_incremental_df, update_keys, logger)

    # --- MERGE LOGIC END ---

    final_row_count = len(updated_df)
    net_change = final_row_count - initial_row_count

    logger.debug(f'  Final row count: {final_row_count:,}')
    logger.debug(f'  Net change: {net_change:+,} rows')

    # Apply filters to final merged dataframe (cleans history)
    logger.debug('Applying filters to final database...')
    updated_df = transformation.apply_filters(updated_df, data_config, logger)

    # Write updated parquet ONCE
    logger.info(f'Writing updated parquet: {db_file.name}')
    updated_df.write_parquet(db_file)

    # Validate updated data
    validation_results = validate_parquet_data(updated_df, blank_vpn_permitted_file, logger)

    # Archive files if archive folder specified
    if archive_folder:
        should_archive = config.get('file_patterns', {}).get('0031_incremental', {}).get('archive_after_processing', True)
        if should_archive:
            logger.info('Archiving processed files...')
            for file_path in incremental_files:
                archive_file(file_path, archive_folder)

    logger.info('✓ Incremental update completed successfully')

    return updated_df, validation_results, change_results



def process_full_backup(
    full_backup_files: list[Path],
    config: dict,
    paths: dict,
    logger: logging.Logger | None = None,
) -> tuple[pl.DataFrame, dict]:
    """
    Process a full backup (e.g. 8 plant files) and rebuild the database

    Args:
        full_backup_files: List of full backup Excel files
        config: Configuration dictionary
        paths: Paths dictionary
        logger: Logger instance

    Returns:
        Tuple of (DataFrame, validation_results)
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    db_file = paths['db_file_path']
    blank_vpn_permitted_file = paths.get('blank_vpn_permitted_file')

    logger.info(f'=== Processing Full Backup ({len(full_backup_files)} files) ===')
    start_time = time.time()

    infer_schema_length = config.get('processing_options', {}).get('infer_schema_length', 0)

    # Use ingest module
    final_df = ingest.process_excel_files(full_backup_files, infer_schema_length, logger)

    if final_df is None:
        raise ValueError('Failed to process full backup files')

    # Use transformation module
    final_df = transformation.clean_dataframe(final_df, logger)
    data_config = config.get('data_processing', {})
    final_df = transformation.convert_and_optimize_columns(final_df, config, logger)
    final_df = transformation.apply_filters(final_df, data_config, logger)

    logger.debug(f'Final DataFrame shape: {final_df.shape}')
    logger.info(f'Writing to: {db_file}')
    final_df.write_parquet(db_file)

    # Validate data after writing
    validation_results = validate_parquet_data(final_df, blank_vpn_permitted_file, logger)

    process_time = time.time() - start_time
    logger.info(f'Full backup processing completed in {process_time:.2f} seconds')

    return final_df, validation_results



