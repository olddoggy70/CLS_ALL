"""
Baseline module for loading 0031 baseline data and creating lookup tables.
"""

import logging
import time
import polars as pl
from ..constants import Columns0031, DailyColumns


def prepare_database_dataframe(config: dict, paths: dict, logger: logging.Logger | None = None) -> pl.DataFrame:
    """Load and prepare the database DataFrame with filters and transformations."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    logger.info('Loading 0031 database...')
    start_time = time.time()

    # Define mirror pairs
    mirror_map = {'0201': '0204', '0204': '0201', '0501': '0504', '0504': '0501'}

    db_df = (
        pl.scan_parquet(paths['db_file_path'])
        .filter(~pl.col(Columns0031.PMM_ITEM_NUMBER).str.starts_with('PM'))
        .filter(~pl.col(Columns0031.VENDOR_NAME).str.starts_with('ZZZ'))
        .drop(config['data_processing']['columns_to_drop'])
        .unique()
        .collect()
    )

    logger.debug(f'  After filters: {len(db_df):,} rows')

    # Add mirror column
    db_df_with_mirror = db_df.with_columns(pl.col(Columns0031.CORP_ACCT).replace_strict(mirror_map, default=None).alias(f'mirror_{Columns0031.CORP_ACCT}'))

    # Find mirrored pairs
    mirrored_pairs = db_df_with_mirror.join(
        db_df_with_mirror.select([Columns0031.PMM_ITEM_NUMBER, Columns0031.VENDOR_CATALOGUE, Columns0031.CORP_ACCT]),
        left_on=[Columns0031.PMM_ITEM_NUMBER, Columns0031.VENDOR_CATALOGUE, f'mirror_{Columns0031.CORP_ACCT}'],
        right_on=[Columns0031.PMM_ITEM_NUMBER, Columns0031.VENDOR_CATALOGUE, Columns0031.CORP_ACCT],
        how='semi',
    ).select([Columns0031.PMM_ITEM_NUMBER, Columns0031.VENDOR_CATALOGUE, Columns0031.CORP_ACCT])

    # Remove only 0204/0504 that are part of mirrored pairs
    db_df = db_df.join(
        mirrored_pairs.filter(pl.col(Columns0031.CORP_ACCT).is_in(['0204', '0504'])),
        on=[Columns0031.PMM_ITEM_NUMBER, Columns0031.VENDOR_CATALOGUE, Columns0031.CORP_ACCT],
        how='anti',
    )

    # Add transformations
    db_df = db_df.with_columns([pl.col(Columns0031.CORP_ACCT).str.slice(0, 2).alias('Corp_Acct_Prefix')]).select(
        pl.all().name.prefix('0031_')
    )

    process_time = time.time() - start_time
    logger.debug(f'  Database ready: {len(db_df):,} rows, {len(db_df.columns)} columns ({process_time:.2f}s)')

    return db_df


def create_lookup_tables(db_df: pl.DataFrame, logger: logging.Logger | None = None) -> tuple:
    """Create various lookup tables from the database DataFrame."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    logger.info('Creating lookup tables...')

    # PMM mapping tables
    pmm_map_dpn = db_df.select(
        [pl.col(f'0031_{Columns0031.VENDOR_CATALOGUE}').alias(DailyColumns.DISTRIBUTOR_PART_NUMBER), f'0031_{Columns0031.PMM_ITEM_NUMBER}']
    ).unique()

    pmm_map_mpn = db_df.select(
        [pl.col(f'0031_{Columns0031.MANUFACTURER_CATALOGUE}').alias(DailyColumns.MANUFACTURER_PART_NUMBER), f'0031_{Columns0031.PMM_ITEM_NUMBER}']
    ).unique()

    pmm_to_desc = db_df.select([f'0031_{Columns0031.PMM_ITEM_NUMBER}', f'0031_{Columns0031.ITEM_DESCRIPTION}']).unique()

    # Contract database
    contract_db_df = (
        db_df.select(
            [
                f'0031_{Columns0031.CONTRACT_NO}',
                f'0031_{Columns0031.CONTRACT_EFF_DATE}',
                f'0031_{Columns0031.CONTRACT_EXP_DATE}',
                f'0031_{Columns0031.CONTRACT_ITEM}',
                f'0031_{Columns0031.VENDOR_CODE}',
                f'0031_{Columns0031.VENDOR_NAME}',
            ]
        )
        .filter(pl.col(f'0031_{Columns0031.CONTRACT_ITEM}') == 'Y')
        .unique()
    )

    # Vendor sequence lookup
    vendor_seq_lookup = (
        db_df.unique()
        .sort(f'0031_{Columns0031.PMM_ITEM_NUMBER}', f'0031_{Columns0031.VENDOR_SEQ}')
        .group_by([f'0031_{Columns0031.PMM_ITEM_NUMBER}', '0031_Corp_Acct_Prefix'])
        .agg([((pl.col(f'0031_{Columns0031.VENDOR_SEQ}').cast(pl.Utf8) + '-' + pl.col(f'0031_{Columns0031.VENDOR_CODE}')).alias('seq_code_pairs'))])
        .with_columns(pl.col('seq_code_pairs').list.join(', ').alias('0031_Vendors List'))
        .drop('seq_code_pairs')
    )

    # Vendor detail lookup
    vendor_detail_lookup = db_df.drop('0031_Item Description').unique()

    logger.debug(f'  Created 6 lookup tables')

    return (pmm_map_dpn, pmm_map_mpn, pmm_to_desc, contract_db_df, vendor_seq_lookup, vendor_detail_lookup)
