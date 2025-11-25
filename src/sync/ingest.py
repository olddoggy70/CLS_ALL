"""
Data ingestion operations for Phase 0 (Sync)
"""

import logging
from pathlib import Path

import polars as pl


def process_excel_files(
    file_paths: list[Path], infer_schema_length: int = 0, logger: logging.Logger | None = None
) -> pl.DataFrame | None:
    """
    Process multiple Excel files and concatenate them into a single DataFrame

    Args:
        file_paths: List of paths to Excel files
        infer_schema_length: Polars inference length
        logger: Logger instance

    Returns:
        Concatenated DataFrame or None if no files processed
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    logger.info(f'Processing {len(file_paths)} Excel files...')
    df_list = []

    for file_path in file_paths:
        try:
            df = pl.read_excel(file_path, infer_schema_length=infer_schema_length)
            # Normalize all string columns (strip)
            df = df.with_columns(pl.col(pl.Utf8).str.strip_chars())
            df_list.append(df)
            logger.debug(f'  {file_path.name} - {df.shape}')
        except Exception as e:
            logger.debug(f'  Failed to read {file_path}: {e}')

    if not df_list:
        return None

    logger.debug('Concatenating dataframes...')
    return pl.concat(df_list, how='diagonal')
