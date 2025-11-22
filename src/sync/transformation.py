"""
Data transformation operations for Phase 0 (Sync)
"""

import logging

import polars as pl

from ..constants import Columns0031


def clean_dataframe(df: pl.DataFrame, logger: logging.Logger | None = None) -> pl.DataFrame:
    """
    Clean DataFrame by trimming strings, converting blanks to None, and trimming column names

    Args:
        df: DataFrame to clean
        logger: Logger instance

    Returns:
        Cleaned DataFrame
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    logger.debug('Cleaning DataFrame...')

    # Trim all string columns and convert blank strings to None
    df = df.with_columns(pl.col(pl.Utf8).str.strip_chars().replace('', None))

    # Trim all column names
    df = df.rename({col: col.strip() for col in df.columns})

    return df


def convert_and_optimize_columns(df: pl.DataFrame, config: dict, logger: logging.Logger | None = None) -> pl.DataFrame:
    """
    Convert date columns and optimize data types based on Schema0031
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    logger.debug('Enforcing schema and optimizing types...')
    
    from ..constants import Schema0031
    
    # 1. Cast columns to expected types defined in Schema0031
    for col_name, dtype in Schema0031.SCHEMA.items():
        if col_name in df.columns:
            current_type = df.schema[col_name]
            if current_type != dtype:
                try:
                    # Special handling for Dates from String
                    if dtype == pl.Date and current_type == pl.Utf8:
                        df = df.with_columns(
                            pl.col(col_name).str.to_date('%Y-%m-%d', strict=False) # Try ISO first
                            .fill_null(pl.col(col_name).str.to_date('%m/%d/%Y', strict=False)) # Try US format
                            .fill_null(pl.col(col_name).str.to_date('%Y-%b-%d', strict=False)) # Try 2025-Nov-10
                            .alias(col_name)
                        )
                    # Special handling for Numeric from String (remove commas, etc if needed, though usually handled by read_excel)
                    else:
                        df = df.with_columns(pl.col(col_name).cast(dtype, strict=False))
                except Exception as e:
                    logger.warning(f'Failed to cast {col_name} to {dtype}: {e}')

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


def apply_filters(df: pl.DataFrame, config: dict, logger: logging.Logger | None = None) -> pl.DataFrame:
    """
    Apply row filtering based on configuration rules.

    Args:
        df: DataFrame to filter
        config: Configuration dictionary containing 'filter_rules'
        logger: Logger instance

    Returns:
        Filtered DataFrame
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    filter_rules = config.get('filter_rules', {})
    exclude_corp_acct = filter_rules.get('exclude_corp_acct', [])

    if not exclude_corp_acct:
        return df

    # Filter by Corp Acct
    if Columns0031.CORP_ACCT in df.columns:
        initial_count = len(df)

        # Ensure Corp Acct is string for comparison
        df = df.filter(~pl.col(Columns0031.CORP_ACCT).cast(pl.Utf8).is_in(exclude_corp_acct))

        removed_count = initial_count - len(df)

        if removed_count > 0:
            logger.debug(f'  Removed {removed_count} rows with Corp Acct in {exclude_corp_acct}')

    return df
