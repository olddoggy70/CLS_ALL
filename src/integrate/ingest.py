"""
Ingestion module for loading and cleaning daily files.
"""

import logging
import polars as pl
from ..constants import Columns0031, DailyColumns


def process_daily_data(config: dict, paths: dict, logger: logging.Logger | None = None) -> pl.DataFrame:
    """Process daily Excel files."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    logger.info('Processing daily files...')

    daily_files = list(paths['daily_files_folder'].glob('*.xlsx'))

    if not daily_files:
        raise ValueError('No daily Excel files found in daily_files folder')

    logger.debug(f'Found {len(daily_files)} daily file(s)')

    # Process each file separately to add source tracking and per file index
    df_list = []
    for file_path in daily_files:
        try:
            df = pl.read_excel(file_path, infer_schema_length=config['processing_options']['infer_schema_length'])
            df = df.with_columns(pl.lit(file_path.stem).alias(DailyColumns.SOURCE_FILE))
            df = df.with_row_index(Columns0031.INDEX, offset=1)
            df_list.append(df)
            logger.debug(f'  Loaded {file_path.name}: {len(df):,} rows')
        except Exception as e:
            logger.warning(f'  Failed to read {file_path.name}: {e}')

    if not df_list:
        raise ValueError('Failed to read any Excel files')

    daily_df = pl.concat(df_list)

    # Convert date columns
    daily_df = _convert_date_columns(daily_df, config['data_processing']['daily_date_columns'], logger)

    # Move Source_file into the first column
    daily_df = daily_df.select([DailyColumns.SOURCE_FILE] + [col for col in daily_df.columns if col != DailyColumns.SOURCE_FILE])

    logger.debug(f'Daily data: {len(daily_df):,} rows, {len(daily_df.columns)} columns')
    return daily_df


def _convert_date_columns(df: pl.DataFrame, date_columns: list[str], logger: logging.Logger | None = None) -> pl.DataFrame:
    """Convert string columns to date format using multiple date patterns."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

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
