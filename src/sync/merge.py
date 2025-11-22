"""
Database merging operations for Phase 0 (Sync)
"""

import logging

import polars as pl

from ..constants import Columns0031


def deduplicate_data(
    df: pl.DataFrame, unique_keys: list[str], logger: logging.Logger | None = None
) -> tuple[pl.DataFrame, int]:
    """
    Deduplicate DataFrame keeping the last occurrence based on unique keys

    Args:
        df: DataFrame to deduplicate
        unique_keys: List of column names to use as unique keys
        logger: Logger instance

    Returns:
        Tuple of (Deduplicated DataFrame, Number of duplicates removed)
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    logger.info('Deduplicating for database merge...')
    initial_rows = len(df)

    if Columns0031.ITEM_UPDATE_DATE in df.columns:
        df = df.sort(Columns0031.ITEM_UPDATE_DATE)

    df = df.unique(subset=unique_keys, keep='last')

    deduplicated_rows = len(df)
    duplicates_removed = initial_rows - deduplicated_rows

    if duplicates_removed > 0:
        logger.debug(f'  After deduplication: {deduplicated_rows:,} unique rows ({duplicates_removed:,} duplicates removed)')
    else:
        logger.debug(f'  No duplicates to remove: {deduplicated_rows:,} unique rows')

    return df, duplicates_removed


def prepare_merge_keys(df: pl.DataFrame, logger: logging.Logger | None = None) -> pl.DataFrame:
    """
    Create unique merge keys for database processing

    Args:
        df: DataFrame to add merge keys to
        logger: Logger instance

    Returns:
        DataFrame with _merge_key column
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    # logger.debug('Creating merge keys...')
    return df.with_columns(
        pl.concat_str(
            [
                pl.col(Columns0031.PMM_ITEM_NUMBER).cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col(Columns0031.CORP_ACCT).cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col(Columns0031.VENDOR_CODE).cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col(Columns0031.ADD_COST_CENTRE).cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col(Columns0031.ADD_GL_ACCOUNT).cast(pl.Utf8).fill_null('').str.strip_chars(),
            ],
            separator='|',
        ).alias('_merge_key')
    )


def identify_changes(
    current_df: pl.DataFrame, incremental_df: pl.DataFrame, logger: logging.Logger | None = None
) -> tuple[set, set]:
    """
    Identify updates vs new records based on merge keys

    Args:
        current_df: Existing database DataFrame (must have _merge_key)
        incremental_df: New incremental DataFrame (must have _merge_key)
        logger: Logger instance

    Returns:
        Tuple of (Set of keys to update, Set of new keys)
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    logger.debug('Analyzing database changes...')
    current_keys = set(current_df.select('_merge_key').to_series().to_list())
    incremental_keys = set(incremental_df.select('_merge_key').to_series().to_list())

    update_keys = current_keys & incremental_keys
    new_keys = incremental_keys - current_keys

    logger.debug(f'  Rows to update: {len(update_keys):,}')
    logger.debug(f'  New rows to add: {len(new_keys):,}')

    return update_keys, new_keys


def check_duplicate_keys(
    current_df: pl.DataFrame, update_keys: set, logger: logging.Logger | None = None
):
    """
    Check for duplicate keys in the existing database that are about to be updated

    Args:
        current_df: Existing database DataFrame
        update_keys: Set of keys that will be updated
        logger: Logger instance
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

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

            # Log the actual merge keys being cleaned
            logger.info('  Duplicate keys being cleaned:')
            for merge_key in sorted(list(duplicates_being_updated))[:10]:  # Show first 10
                logger.info(f'    - {merge_key}')
            if len(duplicates_being_updated) > 10:
                logger.info(f'    ... and {len(duplicates_being_updated) - 10} more')


def merge_dataframes(
    current_df: pl.DataFrame,
    incremental_df: pl.DataFrame,
    update_keys: set,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    """
    Merge incremental data into current database

    Args:
        current_df: Existing database DataFrame
        incremental_df: New incremental DataFrame
        update_keys: Set of keys to update (remove from current before adding incremental)
        logger: Logger instance

    Returns:
        Merged DataFrame
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    logger.info('Merging with database...')
    if update_keys:
        updated_df = current_df.filter(~pl.col('_merge_key').is_in(list(update_keys)))
        removed_count = len(current_df) - len(updated_df)
        logger.debug(f'  Rows removed from database: {removed_count:,}')
        logger.debug(f'  Expected to remove: {len(update_keys):,}')
    else:
        updated_df = current_df

    updated_df = pl.concat([updated_df, incremental_df], how='diagonal')

    # Remove temporary merge key
    updated_df = updated_df.drop('_merge_key')

    return updated_df
