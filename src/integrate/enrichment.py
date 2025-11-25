"""
Enrichment module for applying business logic and joining data.
"""

import logging
import polars as pl
from ..constants import Columns0031, DailyColumns


def enrich_daily_data(daily_df: pl.DataFrame, lookup_tables: tuple, logger: logging.Logger | None = None) -> pl.DataFrame:
    """Enrich daily data with PMM mappings and vendor information."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    logger.info('Enriching with PMM mappings and vendor data...')

    (pmm_map_dpn, pmm_map_mpn, pmm_to_desc, contract_db_df, vendor_seq_lookup, vendor_detail_lookup) = lookup_tables

    # Initial PMM mapping
    daily_enriched = daily_df.join(
        pmm_map_dpn.rename({f'0031_{Columns0031.PMM_ITEM_NUMBER}': 'PMM_by_DPN'}), on=DailyColumns.DISTRIBUTOR_PART_NUMBER, how='left'
    ).join(pmm_map_mpn.rename({f'0031_{Columns0031.PMM_ITEM_NUMBER}': 'PMM_by_MPN'}), on=DailyColumns.MANUFACTURER_PART_NUMBER, how='left')

    # Collapse PMM candidates
    daily_enriched = _collapse_pmm_candidates(daily_enriched, logger)

    # Add vendor information
    daily_enriched = (
        daily_enriched.with_columns(pl.col(DailyColumns.PLANT_ID).str.slice(0, 2).alias('plant_prefix'))
        .join(pmm_to_desc, left_on=DailyColumns.PMM, right_on=f'0031_{Columns0031.PMM_ITEM_NUMBER}', how='left')
        .join(
            vendor_seq_lookup,
            left_on=[DailyColumns.PMM, 'plant_prefix'],
            right_on=[f'0031_{Columns0031.PMM_ITEM_NUMBER}', '0031_Corp_Acct_Prefix'],
            how='left',
        )
        .join(
            vendor_detail_lookup,
            left_on=[DailyColumns.PMM, 'plant_prefix', DailyColumns.DISTRIBUTOR],
            right_on=[f'0031_{Columns0031.PMM_ITEM_NUMBER}', '0031_Corp_Acct_Prefix', f'0031_{Columns0031.VENDOR_CODE}'],
            how='left',
            coalesce=False,
        )
        .drop('plant_prefix', f'0031_{Columns0031.PMM_ITEM_NUMBER}', '0031_Corp_Acct_Prefix')
    )

    logger.debug(f'  Enriched: {len(daily_enriched):,} rows, {len(daily_enriched.columns)} columns')
    return daily_enriched


def _collapse_pmm_candidates(df: pl.DataFrame, logger: logging.Logger | None = None) -> pl.DataFrame:
    """
    Collapse multiple PMM candidates per index, showing combined sources.
    Removes blank/null PMM rows if the same index has non-blank PMM values.
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    logger.debug('Collapsing PMM candidates...')

    # Ensure PMM columns are same type (str)
    df = df.with_columns([pl.col('PMM_by_DPN').cast(pl.Utf8), pl.col('PMM_by_MPN').cast(pl.Utf8)])

    dpn_df = df.select(
        [
            pl.exclude(['PMM_by_DPN', 'PMM_by_MPN']),
            pl.col('PMM_by_DPN').alias(DailyColumns.PMM),
            pl.when(pl.col('PMM_by_DPN').is_not_null() & (pl.col('PMM_by_DPN') != ''))
            .then(pl.lit('DPN'))
            .otherwise(pl.lit(''))
            .alias('PMM_source'),
        ]
    )

    mpn_df = df.select(
        [
            pl.exclude(['PMM_by_DPN', 'PMM_by_MPN']),
            pl.col('PMM_by_MPN').alias(DailyColumns.PMM),
            pl.when(pl.col('PMM_by_MPN').is_not_null() & (pl.col('PMM_by_MPN') != ''))
            .then(pl.lit('MPN'))
            .otherwise(pl.lit(''))
            .alias('PMM_source'),
        ]
    )

    # Concatenate and group
    other_cols = [col for col in df.columns if col not in ['PMM_by_DPN', 'PMM_by_MPN']]
    groupby_cols = [*other_cols, DailyColumns.PMM]

    result = (
        pl.concat([dpn_df, mpn_df])
        .group_by(groupby_cols)
        .agg(pl.col('PMM_source').unique().sort().str.join(',').alias('PMM_source'))
    )

    # Remove blank PMM rows if non-blank exists
    result = result.with_columns(pl.col(DailyColumns.PMM).fill_null(''))

    has_non_blank_pmm = (
        result.filter(pl.col(DailyColumns.PMM) != '').select(other_cols).unique().with_columns(pl.lit(True).alias('has_non_blank'))
    )

    result = (
        result.join(has_non_blank_pmm, on=other_cols, how='left')
        .with_columns(pl.col('has_non_blank').fill_null(False))
        .filter((pl.col(DailyColumns.PMM) != '') | (~pl.col('has_non_blank')))
        .drop('has_non_blank')
        .sort([*other_cols, DailyColumns.PMM])
    )

    return result


def add_contract_analysis(df: pl.DataFrame, contract_db_df: pl.DataFrame, logger: logging.Logger | None = None) -> pl.DataFrame:
    """Add contract-related analysis columns."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    logger.debug('Adding contract analysis...')

    df = df.with_columns(
        pl.when(
            (pl.col(Columns0031.CONTRACT_START) == pl.col(f'0031_{Columns0031.CONTRACT_EFF_DATE}'))
            & (pl.col(Columns0031.CONTRACT_END) == pl.col(f'0031_{Columns0031.CONTRACT_EXP_DATE}'))
        )
        .then(pl.lit('Date Matched'))
        .when(pl.col(f'0031_{Columns0031.CONTRACT_EFF_DATE}').is_null())
        .then(pl.lit('New Contract'))
        .when(
            (pl.col(Columns0031.CONTRACT_START) != pl.col(f'0031_{Columns0031.CONTRACT_EFF_DATE}'))
            & (pl.col(Columns0031.CONTRACT_END) != pl.col(f'0031_{Columns0031.CONTRACT_EXP_DATE}'))
        )
        .then(pl.lit('Start & End Date not Match'))
        .when(pl.col(Columns0031.CONTRACT_START) != pl.col(f'0031_{Columns0031.CONTRACT_EFF_DATE}'))
        .then(pl.lit('Start Date not Match'))
        .when(pl.col(Columns0031.CONTRACT_END) != pl.col(f'0031_{Columns0031.CONTRACT_EXP_DATE}'))
        .then(pl.lit('End Date not Match'))
        .otherwise(pl.lit('Exception'))
        .alias('Contract Header Check')
    )

    df = df.with_columns(pl.lit(None).alias('Suggested Contract No.'))

    return df


def add_reference_mappings(df: pl.DataFrame, config: dict, paths: dict, logger: logging.Logger | None = None) -> pl.DataFrame:
    """Add MFN and VN reference mappings."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    logger.info('Adding reference mappings...')

    # Load reference files
    mfn_ref = pl.read_excel(paths['mfn_mapping_file'], infer_schema_length=config['processing_options']['infer_schema_length'])
    vn_ref = pl.read_excel(paths['vn_mapping_file'], infer_schema_length=config['processing_options']['infer_schema_length'])

    # Add MFN mapping
    df = df.join(
        mfn_ref.select([pl.col('All_Manufacturer Number').alias('Suggested AllScripts MFN'), pl.col('BC_Manufacturer No_')]),
        left_on='ERP Manufacturer No.',
        right_on='BC_Manufacturer No_',
        how='left',
    )

    # Add VN mapping
    df = df.join(
        vn_ref.select([pl.col('All_Vendor Code').alias('Suggested AllScripts VN'), pl.col('BC_Vendor No_')]),
        left_on='MMC Distributor No.',
        right_on='BC_Vendor No_',
        how='left',
    )

    logger.debug(f'  Added MFN and VN mappings')

    return df


def add_highest_uom_price(df: pl.DataFrame, logger: logging.Logger | None = None) -> pl.DataFrame:
    """
    Adds a column 'Highest_UOM_Price' to the dataframe based on UOM conversions.
    Handles string columns by casting to Float32 (matching config.json type_optimization).
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    logger.debug('Calculating highest UOM price...')

    # Step 1: Cast quantity and price columns to numeric (Float32 to match config)
    df = df.with_columns(
        [
            pl.col('AUOM1 QTY').str.replace_all(',', '').cast(pl.Float32, strict=False),
            pl.col('AUOM2 QTY').str.replace_all(',', '').cast(pl.Float32, strict=False),
            pl.col('AUOM3 QTY').str.replace_all(',', '').cast(pl.Float32, strict=False),
            pl.col(Columns0031.PURCHASE_UOM_PRICE).str.replace_all(',', '').cast(pl.Float32, strict=False),
        ]
    )

    # Step 2: Find the quantity for the Purchase Order Price UOM
    purchase_qty = (
        pl.when(pl.col('Purchase Order Price Unit of Measure') == pl.col('Base Unit of Measure'))
        .then(pl.lit(1.0))
        .when(pl.col('Purchase Order Price Unit of Measure') == pl.col('AUOM1'))
        .then(pl.col('AUOM1 QTY'))
        .when(pl.col('Purchase Order Price Unit of Measure') == pl.col('AUOM2'))
        .then(pl.col('AUOM2 QTY'))
        .when(pl.col('Purchase Order Price Unit of Measure') == pl.col('AUOM3'))
        .then(pl.col('AUOM3 QTY'))
        .otherwise(pl.lit(1.0))
        .fill_null(1.0)
    )

    # Step 3: Calculate price per base unit
    price_per_base = pl.col(Columns0031.PURCHASE_UOM_PRICE) / purchase_qty

    # Step 4: Calculate prices for each UOM level
    base_price = price_per_base
    auom1_price = price_per_base * pl.col('AUOM1 QTY').fill_null(0.0)
    auom2_price = price_per_base * pl.col('AUOM2 QTY').fill_null(0.0)
    auom3_price = price_per_base * pl.col('AUOM3 QTY').fill_null(0.0)

    # Step 5: Find the maximum price across all UOMs
    df = df.with_columns([pl.max_horizontal([base_price, auom1_price, auom2_price, auom3_price]).alias('Highest_UOM_Price')])

    return df
