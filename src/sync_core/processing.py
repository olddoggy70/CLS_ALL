"""
Data processing operations - cleaning, incremental updates, and full rebuilds
"""

import logging
import shutil
import time
from pathlib import Path

import polars as pl

from .backup import create_backup

# Import from other sync_core modules
from .files import archive_file, cleanup_old_full_backups, get_excel_files
from .quality import track_row_changes, validate_parquet_data


def process_excel_files(
    file_paths: list[Path], infer_schema_length: int = 0, logger: logging.Logger | None = None
) -> pl.DataFrame | None:
    """Process multiple Excel files and concatenate them into a single DataFrame"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.processing')

    logger.info(f'Processing {len(file_paths)} Excel files...')
    df_list = []

    for file_path in file_paths:
        try:
            df = pl.read_excel(file_path, infer_schema_length=infer_schema_length)
            # Normalize all string columns (strip)
            df = df.with_columns(
                [
                    pl.when(pl.col(c).dtype == pl.Utf8).then(pl.col(c).str.strip_chars()).otherwise(pl.col(c)).alias(c)
                    for c in df.columns
                ]
            )
            df_list.append(df)
            logger.debug(f'  {file_path.name} - {df.shape}')
        except Exception as e:
            logger.debug(f'  Failed to read {file_path}: {e}')

    if not df_list:
        return None

    logger.debug('Concatenating dataframes...')
    return pl.concat(df_list, how='diagonal')


def clean_dataframe(df: pl.DataFrame, logger: logging.Logger | None = None) -> pl.DataFrame:
    """Clean DataFrame by trimming strings, converting blanks to None, and trimming column names"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.processing')

    logger.debug('Cleaning DataFrame...')

    # Trim all string columns and convert blank strings to None
    df = df.select(
        [df[col].str.strip_chars().replace('', None).alias(col) if df[col].dtype == pl.Utf8 else df[col] for col in df.columns]
    )

    # Trim all column names
    df = df.rename({col: col.strip() for col in df.columns})

    return df


def convert_and_optimize_columns(df: pl.DataFrame, config: dict, logger: logging.Logger | None = None) -> pl.DataFrame:
    """
    Convert date columns and optimize data types for memory and performance

    Note: Does NOT convert categorical columns - those are applied at read time
    in orchestrator.py for in-memory performance only (not persisted to Parquet)

    Args:
        df: DataFrame to process
        config: Configuration dictionary with date_columns and type_optimization
        logger: Logger instance

    Returns:
        DataFrame with converted dates and optimized numeric types
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.processing')

    logger.debug('Converting dates and optimizing data types...')

    # 1. Convert date columns
    date_columns = config.get('date_columns', [])
    if date_columns:
        date_cols_present = [col for col in date_columns if col in df.columns]
        if date_cols_present:
            logger.debug(f'  Converting {len(date_cols_present)} date columns...')
            df = df.with_columns(
                [
                    (
                        pl.when(pl.col(col).str.to_date('%Y-%m-%d', strict=False).is_not_null())
                        .then(pl.col(col).str.to_date('%Y-%m-%d', strict=False))
                        .when(pl.col(col).str.to_date('%m/%d/%Y', strict=False).is_not_null())
                        .then(pl.col(col).str.to_date('%m/%d/%Y', strict=False))
                        .when(pl.col(col).str.to_date('%Y-%b-%d', strict=False).is_not_null())
                        .then(pl.col(col).str.to_date('%Y-%b-%d', strict=False))
                        .otherwise(None)
                        .alias(col)
                    )
                    for col in date_cols_present
                ]
            )

    # 2. Optimize float32 columns (numeric optimization)
    type_opt = config.get('type_optimization', {})
    float32_columns = type_opt.get('float32_columns', [])
    if float32_columns:
        float32_cols_present = [col for col in float32_columns if col in df.columns]
        if float32_cols_present:
            logger.debug(f'  Optimizing {len(float32_cols_present)} float32 columns...')
            df = df.with_columns([pl.col(col).cast(pl.Float32) for col in float32_cols_present])

    # Note: Categorical columns are NOT converted here
    # They are applied at read time in orchestrator.py for runtime performance only

    return df

def apply_categorical_types(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    """
    Apply categorical types to DataFrame for in-memory performance

    This should be called AFTER reading from Parquet to get performance benefits
    without the storage penalty. Parquet does not have native categorical type.

    Args:
        df: DataFrame to cast
        config: Configuration dictionary with type_optimization.categorical_columns

    Returns:
        DataFrame with categorical columns cast
    """
    type_opt = config.get('data_processing', {}).get('type_optimization', {})
    categorical_columns = type_opt.get('categorical_columns', [])

    if categorical_columns:
        cat_cols_present = [col for col in categorical_columns if col in df.columns]
        if cat_cols_present:
            df = df.with_columns([pl.col(col).cast(pl.Categorical) for col in cat_cols_present])

    return df


def convert_date_columns(df: pl.DataFrame, date_columns: list[str], logger: logging.Logger | None = None) -> pl.DataFrame:
    """
    DEPRECATED: Use convert_and_optimize_columns() instead
    Convert string columns to date format using multiple date patterns

    This function is kept for backward compatibility only.
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.processing')

    logger.debug('Converting date columns...')

    df = df.with_columns(
        [
            (
                pl.when(pl.col(col).str.to_date('%Y-%m-%d', strict=False).is_not_null())
                .then(pl.col(col).str.to_date('%Y-%m-%d', strict=False))
                .when(pl.col(col).str.to_date('%m/%d/%Y', strict=False).is_not_null())
                .then(pl.col(col).str.to_date('%m/%d/%Y', strict=False))
                .when(pl.col(col).str.to_date('%Y-%b-%d', strict=False).is_not_null())
                .then(pl.col(col).str.to_date('%Y-%b-%d', strict=False))
                .otherwise(None)
                .alias(col)
            )
            for col in date_columns
            if col in df.columns
        ]
    )

    return df


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

    Handles both single-file and batch processing:
    - Single file: apply_incremental_update(db_file, single_file_path, ...)
    - Multiple files: apply_incremental_update(db_file, [file1, file2, file3], ...)

    USE CASES:
    - Single file: Manual processing, testing, legacy CLI mode
    - Multiple files: Automatic daily batch processing (efficient!)

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
        logger = logging.getLogger('data_pipeline.sync.processing')

    # Normalize input: single file → list with 1 file
    if isinstance(incremental_files, Path):
        incremental_files = [incremental_files]
        is_batch_mode = False
    else:
        is_batch_mode = True

    logger.info(f'=== Applying Incremental Update{"s" if is_batch_mode else ""} ===')
    if is_batch_mode:
        logger.info(f'Processing {len(incremental_files)} file(s) in batch')
    else:
        logger.info(f'Incremental file: {incremental_files[0].name}')

    # Load existing parquet file
    if not db_file.exists():
        raise ValueError(f'Parquet file not found: {db_file}')

    logger.info(f'Loading existing parquet: {db_file.name}')
    current_df = pl.read_parquet(db_file)
    initial_row_count = len(current_df)
    logger.debug(f'  Current rows: {initial_row_count:,}')

    # Create ONE backup before processing
    logger.info('Creating backup before processing...')
    backup_path = create_backup(db_file, backup_folder)

    # Load ALL incremental files (raw, without cleaning yet)
    logger.info(f'Loading {len(incremental_files)} incremental file(s)...')
    all_incremental_dfs = []
    file_metadata = []
    infer_schema_length = config.get('processing_options', {}).get('infer_schema_length', 0)

    for idx, file_path in enumerate(incremental_files, 1):
        try:
            if is_batch_mode:
                logger.info(f'  [{idx}/{len(incremental_files)}] Loading {file_path.name}...')
            else:
                logger.info(f'Loading incremental updates: {file_path.name}')

            df = pl.read_excel(file_path, infer_schema_length=infer_schema_length)
            logger.debug(f'      Rows: {len(df):,}' if is_batch_mode else f'  Incremental rows: {len(df):,}')

            file_metadata.append({'index': idx, 'filename': file_path.name, 'row_count': len(df), 'dataframe': df})

            all_incremental_dfs.append(df)

        except Exception as e:
            logger.error(f'Error loading {file_path.name}: {e}')
            raise ValueError(f'Failed to load incremental file {file_path.name}: {e}')

    # Concatenate all incrementals into ONE combined DataFrame
    logger.info('Combining all incremental data...')
    combined_incremental_df = pl.concat(all_incremental_dfs, how='diagonal')
    total_incremental_rows = len(combined_incremental_df)
    logger.debug(f'  Total incremental rows: {total_incremental_rows:,}')

    # Clean and optimize ONCE on combined data (efficient for both single and batch!)
    logger.debug('Cleaning and optimizing combined data...')
    combined_incremental_df = clean_dataframe(combined_incremental_df)
    data_config = config.get('data_processing', {})
    combined_incremental_df = convert_and_optimize_columns(combined_incremental_df, data_config)

    # Validate required unique key columns
    unique_keys = ['PMM Item Number', 'Corp Acct', 'Vendor Code', 'Additional Cost Centre', 'Additional GL Account']
    if not all(col in current_df.columns for col in unique_keys):
        raise ValueError(f'Current parquet missing required columns: {unique_keys}')

    if not all(col in combined_incremental_df.columns for col in unique_keys):
        raise ValueError(f'Incremental files missing required columns: {unique_keys}')

    # Track changes - different approach for batch vs single file
    if is_batch_mode:
        # PER-FILE tracking for batch mode
        logger.info('Tracking changes per file...')
        all_change_results = []
        current_position = 0

        for file_info in file_metadata:
            file_idx = file_info['index']
            filename = file_info['filename']
            row_count = file_info['row_count']

            file_df = combined_incremental_df.slice(current_position, row_count)
            current_position += row_count

            logger.debug(f'  [{file_idx}/{len(file_metadata)}] Tracking changes from {filename}... ({row_count:,} rows)')

            file_change_results = track_row_changes(file_df, current_df, audit_folder)
            file_change_results['source_file'] = filename
            file_change_results['file_index'] = file_idx

            all_change_results.append(file_change_results)

            if file_change_results.get('has_changes'):
                summary = file_change_results.get('changes_summary', {})
                logger.debug(f'      New: {summary.get("new_rows", 0):,}, Updated: {summary.get("updated_rows", 0):,}')

        # Analyze duplicates for batch mode
        logger.info('Analyzing duplicate items across files...')

        combined_with_keys = combined_incremental_df.with_columns(
            pl.concat_str(
                [
                    pl.col('PMM Item Number').cast(pl.Utf8).fill_null('').str.strip_chars(),
                    pl.col('Corp Acct').cast(pl.Utf8).fill_null('').str.strip_chars(),
                    pl.col('Vendor Code').cast(pl.Utf8).fill_null('').str.strip_chars(),
                    pl.col('Additional Cost Centre').cast(pl.Utf8).fill_null('').str.strip_chars(),
                    pl.col('Additional GL Account').cast(pl.Utf8).fill_null('').str.strip_chars(),
                ],
                separator='|',
            ).alias('_temp_key')
        )

        duplicate_cols = [
            pl.count().alias('occurrence_count'),
            pl.col('PMM Item Number').first().alias('PMM Item Number'),
            pl.col('Corp Acct').first().alias('Corp Acct'),
            pl.col('Vendor Code').first().alias('Vendor Code'),
            pl.col('Additional Cost Centre').first().alias('Additional Cost Centre'),
            pl.col('Additional GL Account').first().alias('Additional GL Account'),
            pl.col('Item Update Date').alias('Update_Dates'),
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
                        'PMM Item Number',
                        'Corp Acct',
                        'Vendor Code',
                        'Additional Cost Centre',
                        'Additional GL Account',
                        'Item Update Date',
                    ]
                )
            )

            duplicates_analysis_df = duplicate_analysis.drop('_temp_key')
            logger.debug('  Duplicate items will be shown in Excel report')
        else:
            logger.debug('  No duplicate items found')

        # Aggregate all change results
        logger.debug('Aggregating changes from all files...')

        all_changes_dfs = [c['changes_df'] for c in all_change_results if c.get('changes_df') is not None]
        all_new_rows_dfs = [c['new_rows_df'] for c in all_change_results if c.get('new_rows_df') is not None]
        all_updated_rows_dfs = [c['updated_rows_df'] for c in all_change_results if c.get('updated_rows_df') is not None]

        combined_changes_df = pl.concat(all_changes_dfs, how='diagonal') if all_changes_dfs else None
        combined_new_rows_df = pl.concat(all_new_rows_dfs, how='diagonal') if all_new_rows_dfs else None
        combined_updated_rows_df = pl.concat(all_updated_rows_dfs, how='diagonal') if all_updated_rows_dfs else None

        per_file_summary = []
        for c in all_change_results:
            summary = c.get('changes_summary', {})
            per_file_summary.append(
                {
                    'file': c['source_file'],
                    'file_index': c['file_index'],
                    'total_changes': summary.get('total_changes', 0),
                    'new_rows': summary.get('new_rows', 0),
                    'updated_rows': summary.get('updated_rows', 0),
                    'latest_update_date': summary.get('latest_update_date', 'N/A'),
                    'date_breakdown': c.get('date_breakdown'),
                }
            )

        aggregated_summary = {
            'total_changes': sum(c.get('changes_summary', {}).get('total_changes', 0) for c in all_change_results),
            'new_rows': sum(c.get('changes_summary', {}).get('new_rows', 0) for c in all_change_results),
            'updated_rows': sum(c.get('changes_summary', {}).get('updated_rows', 0) for c in all_change_results),
            'files_processed': len(all_change_results),
            'per_file_summary': per_file_summary,
        }

        change_results = {
            'has_changes': True,
            'changes_summary': aggregated_summary,
            'changes_df': combined_changes_df,
            'new_rows_df': combined_new_rows_df,
            'updated_rows_df': combined_updated_rows_df,
            'duplicates_summary': {'duplicate_count': duplicate_count, 'total_rows_before_dedup': total_incremental_rows},
            'duplicates_analysis_df': duplicates_analysis_df,
            'duplicates_full_df': duplicates_full_df,
        }
    else:
        # SIMPLE tracking for single file mode
        logger.info('Tracking changes...')
        change_results = track_row_changes(combined_incremental_df, current_df, audit_folder)

    # Deduplicate: Keep last occurrence if same item appears in multiple files
    logger.info('Deduplicating for database merge...')

    if 'Item Update Date' in combined_incremental_df.columns:
        combined_incremental_df = combined_incremental_df.sort('Item Update Date')

    combined_incremental_df = combined_incremental_df.unique(subset=unique_keys, keep='last')

    deduplicated_rows = len(combined_incremental_df)
    duplicates_removed = total_incremental_rows - deduplicated_rows

    if duplicates_removed > 0:
        logger.debug(f'  After deduplication: {deduplicated_rows:,} unique rows ({duplicates_removed:,} duplicates removed)')
    else:
        logger.debug(f'  No duplicates to remove: {deduplicated_rows:,} unique rows')

    # Create unique merge keys for database processing
    logger.debug('Creating merge keys for database update...')
    current_df = current_df.with_columns(
        pl.concat_str(
            [
                pl.col('PMM Item Number').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Corp Acct').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Vendor Code').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional Cost Centre').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional GL Account').cast(pl.Utf8).fill_null('').str.strip_chars(),
            ],
            separator='|',
        ).alias('_merge_key')
    )

    combined_incremental_df = combined_incremental_df.with_columns(
        pl.concat_str(
            [
                pl.col('PMM Item Number').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Corp Acct').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Vendor Code').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional Cost Centre').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional GL Account').cast(pl.Utf8).fill_null('').str.strip_chars(),
            ],
            separator='|',
        ).alias('_merge_key')
    )

    # Identify updates vs new records
    logger.debug('Analyzing database changes...')
    current_keys = set(current_df.select('_merge_key').to_series().to_list())
    incremental_keys = set(combined_incremental_df.select('_merge_key').to_series().to_list())

    update_keys = current_keys & incremental_keys
    new_keys = incremental_keys - current_keys

    logger.debug(f'  Rows to update: {len(update_keys):,}')
    logger.debug(f'  New rows to add: {len(new_keys):,}')

    # After identifying duplicates being updated
    duplicate_keys_in_db = current_df.group_by('_merge_key').agg(pl.count().alias('count')).filter(pl.col('count') > 1)

    if len(duplicate_keys_in_db) > 0:
        logger.warning(f'  WARNING: Found {len(duplicate_keys_in_db)} duplicate keys in database!')

        # Find which duplicate keys are being updated
        duplicate_keys_set = set(duplicate_keys_in_db.select('_merge_key').to_series().to_list())
        duplicates_being_updated = update_keys & duplicate_keys_set

        if duplicates_being_updated:
            # Count total extra rows that will be removed
            extra_rows_removed = (
                current_df.filter(pl.col('_merge_key').is_in(list(duplicates_being_updated)))
                .group_by('_merge_key')
                .agg(pl.count().alias('count'))
                .select((pl.col('count') - 1).sum())
                .item()
            )
            logger.warning(f'  {len(duplicates_being_updated)} duplicate keys will be updated')
            logger.warning(f'  This will remove {extra_rows_removed} extra rows from database')

            # NEW: Log the actual merge keys being cleaned
            logger.info('  Duplicate keys being cleaned:')
            for merge_key in sorted(list(duplicates_being_updated))[:10]:  # Show first 10
                logger.info(f'    - {merge_key}')
            if len(duplicates_being_updated) > 10:
                logger.info(f'    ... and {len(duplicates_being_updated) - 10} more')

    # Apply merge: Remove old versions and add all incremental data
    logger.info('Merging with database...')
    if update_keys:
        updated_df = current_df.filter(~pl.col('_merge_key').is_in(list(update_keys)))
        removed_count = len(current_df) - len(updated_df)
        logger.debug(f'  Rows removed from database: {removed_count:,}')
        logger.debug(f'  Expected to remove: {len(update_keys):,}')
    else:
        updated_df = current_df

    updated_df = pl.concat([updated_df, combined_incremental_df], how='diagonal')

    # Remove temporary merge key
    updated_df = updated_df.drop('_merge_key')

    final_row_count = len(updated_df)
    net_change = final_row_count - initial_row_count

    logger.debug(f'  Final row count: {final_row_count:,}')
    logger.debug(f'  Net change: {net_change:+,} rows')

    # Write updated parquet ONCE
    logger.info(f'Writing updated parquet: {db_file.name}')
    updated_df.write_parquet(db_file)

    # Validate updated data
    validation_results = validate_parquet_data(updated_df, blank_vpn_permitted_file)

    # Archive files if batch mode with archive folder specified
    if archive_folder and is_batch_mode:
        should_archive = config.get('file_patterns', {}).get('daily_incremental', {}).get('archive_after_processing', True)
        if should_archive:
            logger.info('Archiving processed files...')
            for file_path in incremental_files:
                archive_file(file_path, archive_folder)

    logger.info('✓ Incremental update completed successfully')

    return updated_df, validation_results, change_results


def rebuild_parquet(
    main_folder: Path,
    db_file: Path,
    config: dict,
    skip_cleaning: bool = False,
    blank_vpn_permitted_file: Path | None = None,
    logger: logging.Logger | None = None,
) -> tuple[pl.DataFrame, dict]:
    """
    Rebuild parquet file from scratch by processing all Excel files

    Args:
        main_folder: Folder containing Excel files
        db_file: Output parquet file path
        config: Configuration dictionary
        skip_cleaning: If True, skip cleaning and date conversion (just raw data)
        blank_vpn_permitted_file: Path to permitted blank VPN file
        logger: Logger instance

    Returns:
        Tuple of (DataFrame, validation_results)
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.processing')

    logger.info('=== Rebuilding Parquet File ===')
    start_time = time.time()

    excel_files = get_excel_files(main_folder)
    if not excel_files:
        raise ValueError('No Excel files found in main folder')

    infer_schema_length = config.get('processing_options', {}).get('infer_schema_length', 0)
    final_df = process_excel_files(excel_files, infer_schema_length)

    if final_df is None:
        raise ValueError('Failed to process Excel files')

    if not skip_cleaning:
        final_df = clean_dataframe(final_df)
        data_config = config.get('data_processing', {})
        final_df = convert_and_optimize_columns(final_df, data_config)

    logger.debug(f'Final DataFrame shape: {final_df.shape}')
    logger.info(f'Writing to: {db_file}')
    final_df.write_parquet(db_file)

    # Validate data after writing
    validation_results = validate_parquet_data(final_df, blank_vpn_permitted_file)

    process_time = time.time() - start_time
    logger.info(f'Parquet rebuild completed in {process_time:.2f} seconds')

    return final_df, validation_results


def process_weekly_full_backup(
    weekly_file: Path, config: dict, paths: dict, logger: logging.Logger | None = None
) -> tuple[pl.DataFrame, dict]:
    """
    Process weekly full backup file and rebuild parquet from scratch

    Args:
        weekly_file: Path to weekly full backup file
        config: Configuration dictionary
        paths: Paths dictionary
        logger: Logger instance

    Returns:
        Tuple of (DataFrame, validation_results)
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync.processing')

    logger.info('=== Processing Weekly Full Backup ===')
    logger.info(f'Weekly full file: {weekly_file.name}')

    main_folder = paths['main_folder']
    db_file = paths['db_file_path']
    blank_vpn_permitted_file = paths.get('blank_vpn_permitted_file')

    # Create backup of existing parquet before rebuild
    backup_path = create_backup(db_file, paths['backup_folder'])

    # Process the weekly full file as a single-file rebuild
    logger.info('Processing weekly full backup file...')
    try:
        infer_schema_length = config.get('processing_options', {}).get('infer_schema_length', 0)
        final_df = pl.read_excel(weekly_file, infer_schema_length=infer_schema_length)
        logger.debug(f'  Weekly full rows: {len(final_df):,}')

        # Clean and process data
        final_df = clean_dataframe(final_df)
        data_config = config.get('data_processing', {})
        final_df = convert_and_optimize_columns(final_df, data_config)

        # Write to parquet
        logger.info(f'Writing to: {db_file}')
        final_df.write_parquet(db_file)

        # Validate data
        validation_results = validate_parquet_data(final_df, blank_vpn_permitted_file)

        # Cleanup old full backup files (keep only current)
        keep_only_latest = config.get('file_patterns', {}).get('weekly_full', {}).get('keep_only_latest', True)
        if keep_only_latest:
            cleanup_old_full_backups(main_folder, weekly_file, config)

        logger.info('✓ Weekly full backup processed successfully')
        logger.debug(f'  Final rows: {len(final_df):,}')
        logger.debug(f'  Columns: {len(final_df.columns)}')

        return final_df, validation_results

    except Exception as e:
        logger.error(f'Error processing weekly full backup: {e}')

        # Restore from backup if it exists
        if backup_path and backup_path.exists():
            logger.info('Attempting to restore from backup...')
            shutil.copy2(backup_path, db_file)
            logger.info('Backup restored successfully')

        raise
