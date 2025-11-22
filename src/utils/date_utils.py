"""
Date parsing and extraction utilities.

Shared utilities for extracting date ranges and parsing dates
across all pipeline phases.
"""

import logging
from datetime import datetime

import polars as pl


def extract_date_range(df: pl.DataFrame, date_column: str, date_format: str, logger: logging.Logger | None = None) -> str:
    """
    Extract date range from DataFrame for smart filename generation.

    Args:
        df: DataFrame containing date column
        date_column: Name of column containing dates
        date_format: strptime format for parsing dates
        logger: Logger instance

    Returns:
        Date string in one of these formats:
        - 'no_data' if no valid dates found
        - 'YYYY-MM-DD' for single date
        - 'YYYY-MM-DD~YYYY-MM-DD' for date range
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.utils.date_utils')

    try:
        if date_column not in df.columns:
            logger.debug(f'Date column "{date_column}" not found, using "no_data"')
            return 'no_data'

        dates = (
            df.select(
                pl.col(date_column)
                .str.strptime(pl.Datetime, date_format, strict=False)
                .dt.date()
                .dt.strftime('%Y-%m-%d')
                .drop_nulls()
            )
            .unique()
            .sort(by=pl.col(date_column))
            .to_series()
            .to_list()
        )

        if len(dates) == 0:
            logger.debug('No valid dates found, using "no_data"')
            return 'no_data'
        elif len(dates) == 1:
            logger.debug(f'Single date found: {dates[0]}')
            return dates[0]
        else:
            date_range = f'{dates[0]}~{dates[-1]}'
            logger.debug(f'Date range found: {date_range}')
            return date_range
    except Exception as e:
        logger.debug(f'Date extraction failed: {e}, using "no_data"')
        return 'no_data'
