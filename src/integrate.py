"""
Phase 1: Integration
Integrate daily files with 0031 baseline database
"""

import time
from pathlib import Path

import polars as pl


def process_integrate(config: dict, paths: dict) -> bool:
    """
    Phase 1: Integrate daily files with 0031 baseline

    Args:
        config: Configuration dict
        paths: Paths dict

    Returns:
        True if processing succeeded
    """
    print('\n' + '=' * 60)
    print('PHASE 1: INTEGRATION')
    print('Integrate daily files with 0031 baseline')
    print('=' * 60)

    try:
        # Process daily data
        daily_df = _process_daily_data(config, paths)

        # Prepare database DataFrame
        db_df = _prepare_database_dataframe(config, paths)

        # Create lookup tables
        lookup_tables = _create_lookup_tables(db_df)

        # Enrich daily data
        enriched_df = _enrich_daily_data(daily_df, lookup_tables)

        # Add contract analysis
        enriched_df = _add_contract_analysis(enriched_df)

        # Add reference mappings
        enriched_df = _add_reference_mappings(enriched_df, config, paths)

        # Add highest UoM Price
        enriched_df = _add_highest_uom_price(enriched_df)
        
        # Finalize DataFrame
        final_df = _finalize_dataframe(enriched_df)

        # Save integrated output
        output_path = _save_integrated_output(final_df, config, paths)

        print(f'\n✓ Phase 1 completed successfully')
        print(f'  Output: {output_path}')
        print(f'  Shape: {final_df.shape}')

        return True

    except Exception as e:
        print(f'\n!!! Phase 1 failed: {e}')
        raise


def _process_daily_data(config: dict, paths: dict) -> pl.DataFrame:
    """Process daily Excel files."""
    print('\n--- Processing Daily Data ---')

    daily_files = list(paths['daily_files_folder'].glob('*.xlsx'))

    if not daily_files:
        raise ValueError('No daily Excel files found in daily_files folder')
    print(f'Found {len(daily_files)} daily file(s)')

    # Process each file separately to add source tracking and per file index
    df_list = []
    for file_path in daily_files:
        try:
            df = pl.read_excel(file_path, infer_schema_length=config['processing_options']['infer_schema_length'])
            df = df.with_columns(pl.lit(file_path.stem).alias('Source_File'))
            df = df.with_row_index('Index', offset=1)
            df_list.append(df)
            print(f'  {file_path.name} - {df.shape}')
        except Exception as e:
            print(f'  Failed to read {file_path.name}: {e}')

    if not df_list:
        raise ValueError('Failed to read any Excel files')

    daily_df = pl.concat(df_list)

    # Convert date columns
    daily_df = _convert_date_columns(daily_df, config['data_processing']['daily_date_columns'])

    # Move Source_file into the first column
    daily_df = daily_df.select(['Source_File'] + [col for col in daily_df.columns if col != 'Source_File'])

    print(f'Daily DataFrame shape: {daily_df.shape}')
    return daily_df


def _convert_date_columns(df: pl.DataFrame, date_columns: list[str]) -> pl.DataFrame:
    """Convert string columns to date format using multiple date patterns."""
    print('Converting date columns...')

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


def _prepare_database_dataframe(config: dict, paths: dict) -> pl.DataFrame:
    """Load and prepare the database DataFrame with filters and transformations."""
    print('\n--- Preparing Database DataFrame ---')
    start_time = time.time()

    # Define mirror pairs
    mirror_map = {'0201': '0204', '0204': '0201', '0501': '0504', '0504': '0501'}

    db_df = (
        pl.scan_parquet(paths['db_file_path'])
        .filter(~pl.col('PMM Item Number').str.starts_with('PM'))
        .filter(~pl.col('Vendor Name').str.starts_with('ZZZ'))
        .drop(config['data_processing']['columns_to_drop'])
        .unique()
        .collect()
    )

    # Add mirror column
    db_df_with_mirror = db_df.with_columns(pl.col('Corp Acct').replace_strict(mirror_map, default=None).alias('mirror_Corp Acct'))

    # Find mirrored pairs
    mirrored_pairs = db_df_with_mirror.join(
        db_df_with_mirror.select(['PMM Item Number', 'Vendor Catalogue', 'Corp Acct']),
        left_on=['PMM Item Number', 'Vendor Catalogue', 'mirror_Corp Acct'],
        right_on=['PMM Item Number', 'Vendor Catalogue', 'Corp Acct'],
        how='semi',
    ).select(['PMM Item Number', 'Vendor Catalogue', 'Corp Acct'])

    # Remove only 0204/0504 that are part of mirrored pairs
    db_df = db_df.join(
        mirrored_pairs.filter(pl.col('Corp Acct').is_in(['0204', '0504'])),
        on=['PMM Item Number', 'Vendor Catalogue', 'Corp Acct'],
        how='anti',
    )

    # Add transformations
    db_df = db_df.with_columns([pl.col('Corp Acct').str.slice(0, 2).alias('Corp_Acct_Prefix')]).select(
        pl.all().name.prefix('0031_')
    )

    process_time = time.time() - start_time
    print(f'Database preparation completed in {process_time:.2f} seconds')
    print(f'Database DataFrame shape: {db_df.shape}')

    return db_df


def _collapse_pmm_candidates(df: pl.DataFrame) -> pl.DataFrame:
    """
    Collapse multiple PMM candidates per index, showing combined sources.
    Removes blank/null PMM rows if the same index has non-blank PMM values.
    """
    print('Collapsing PMM candidates...')

    # Ensure PMM columns are same type (str)
    df = df.with_columns([pl.col('PMM_by_DPN').cast(pl.Utf8), pl.col('PMM_by_MPN').cast(pl.Utf8)])

    dpn_df = df.select(
        [
            pl.exclude(['PMM_by_DPN', 'PMM_by_MPN']),
            pl.col('PMM_by_DPN').alias('PMM'),
            pl.when(pl.col('PMM_by_DPN').is_not_null() & (pl.col('PMM_by_DPN') != ''))
            .then(pl.lit('DPN'))
            .otherwise(pl.lit(''))
            .alias('PMM_source'),
        ]
    )

    mpn_df = df.select(
        [
            pl.exclude(['PMM_by_DPN', 'PMM_by_MPN']),
            pl.col('PMM_by_MPN').alias('PMM'),
            pl.when(pl.col('PMM_by_MPN').is_not_null() & (pl.col('PMM_by_MPN') != ''))
            .then(pl.lit('MPN'))
            .otherwise(pl.lit(''))
            .alias('PMM_source'),
        ]
    )

    # Concatenate and group
    other_cols = [col for col in df.columns if col not in ['PMM_by_DPN', 'PMM_by_MPN']]
    groupby_cols = [*other_cols, 'PMM']

    result = (
        pl.concat([dpn_df, mpn_df])
        .group_by(groupby_cols)
        .agg(pl.col('PMM_source').unique().sort().str.join(',').alias('PMM_source'))
    )

    # Remove blank PMM rows if non-blank exists
    result = result.with_columns(pl.col('PMM').fill_null(''))

    has_non_blank_pmm = (
        result.filter(pl.col('PMM') != '').select(other_cols).unique().with_columns(pl.lit(True).alias('has_non_blank'))
    )

    result = (
        result.join(has_non_blank_pmm, on=other_cols, how='left')
        .with_columns(pl.col('has_non_blank').fill_null(False))
        .filter((pl.col('PMM') != '') | (~pl.col('has_non_blank')))
        .drop('has_non_blank')
        .sort([*other_cols, 'PMM'])
    )

    return result


def _create_lookup_tables(db_df: pl.DataFrame) -> tuple:
    """Create various lookup tables from the database DataFrame."""
    print('\n--- Creating Lookup Tables ---')

    # PMM mapping tables
    pmm_map_dpn = db_df.select(
        [pl.col('0031_Vendor Catalogue').alias('Distributor Part Number'), '0031_PMM Item Number']
    ).unique()

    pmm_map_mpn = db_df.select(
        [pl.col('0031_Manufacturer Catalogue').alias('Manufacturer Part Number'), '0031_PMM Item Number']
    ).unique()

    pmm_to_desc = db_df.select(['0031_PMM Item Number', '0031_Item Description']).unique()

    # Contract database
    contract_db_df = (
        db_df.select(
            [
                '0031_Contract No',
                '0031_Contract EFF Date',
                '0031_Contract EXP Date',
                '0031_Contract Item',
                '0031_Vendor Code',
                '0031_Vendor Name',
            ]
        )
        .filter(pl.col('0031_Contract Item') == 'Y')
        .unique()
    )

    # Vendor sequence lookup
    vendor_seq_lookup = (
        db_df.unique()
        .sort('0031_PMM Item Number', '0031_Vendor Seq')
        .group_by(['0031_PMM Item Number', '0031_Corp_Acct_Prefix'])
        .agg([((pl.col('0031_Vendor Seq').cast(pl.Utf8) + '-' + pl.col('0031_Vendor Code')).alias('seq_code_pairs'))])
        .with_columns(pl.col('seq_code_pairs').list.join(', ').alias('0031_Vendors List'))
        .drop('seq_code_pairs')
    )

    # Vendor detail lookup
    vendor_detail_lookup = db_df.drop('0031_Item Description').unique()

    print(
        f'Created {len([pmm_map_dpn, pmm_map_mpn, pmm_to_desc, contract_db_df, vendor_seq_lookup, vendor_detail_lookup])} lookup tables'
    )

    return (pmm_map_dpn, pmm_map_mpn, pmm_to_desc, contract_db_df, vendor_seq_lookup, vendor_detail_lookup)


def _enrich_daily_data(daily_df: pl.DataFrame, lookup_tables: tuple) -> pl.DataFrame:
    """Enrich daily data with PMM mappings and vendor information."""
    print('\n--- Enriching Daily Data ---')

    (pmm_map_dpn, pmm_map_mpn, pmm_to_desc, contract_db_df, vendor_seq_lookup, vendor_detail_lookup) = lookup_tables

    # Initial PMM mapping
    daily_enriched = daily_df.join(
        pmm_map_dpn.rename({'0031_PMM Item Number': 'PMM_by_DPN'}), on='Distributor Part Number', how='left'
    ).join(pmm_map_mpn.rename({'0031_PMM Item Number': 'PMM_by_MPN'}), on='Manufacturer Part Number', how='left')

    # Collapse PMM candidates
    daily_enriched = _collapse_pmm_candidates(daily_enriched)

    # Add vendor information
    daily_enriched = (
        daily_enriched.with_columns(pl.col('Plant ID').str.slice(0, 2).alias('plant_prefix'))
        .join(pmm_to_desc, left_on='PMM', right_on='0031_PMM Item Number', how='left')
        .join(
            vendor_seq_lookup,
            left_on=['PMM', 'plant_prefix'],
            right_on=['0031_PMM Item Number', '0031_Corp_Acct_Prefix'],
            how='left',
        )
        .join(
            vendor_detail_lookup,
            left_on=['PMM', 'plant_prefix', 'Distributor'],
            right_on=['0031_PMM Item Number', '0031_Corp_Acct_Prefix', '0031_Vendor Code'],
            how='left',
            coalesce=False,
        )
        .drop('plant_prefix', '0031_PMM Item Number', '0031_Corp_Acct_Prefix')
    )

    print(f'Enriched DataFrame shape: {daily_enriched.shape}')
    return daily_enriched


def _add_contract_analysis(df: pl.DataFrame) -> pl.DataFrame:
    """Add contract-related analysis columns."""
    print('\n--- Adding Contract Analysis ---')

    df = df.with_columns(
        pl.when(
            (pl.col('Contract Start Date') == pl.col('0031_Contract EFF Date'))
            & (pl.col('Contract End Date') == pl.col('0031_Contract EXP Date'))
        )
        .then(pl.lit('Date Matched'))
        .when(pl.col('0031_Contract EFF Date').is_null())
        .then(pl.lit('New Contract'))
        .when(
            (pl.col('Contract Start Date') != pl.col('0031_Contract EFF Date'))
            & (pl.col('Contract End Date') != pl.col('0031_Contract EXP Date'))
        )
        .then(pl.lit('Start & End Date not Match'))
        .when(pl.col('Contract Start Date') != pl.col('0031_Contract EFF Date'))
        .then(pl.lit('Start Date not Match'))
        .when(pl.col('Contract End Date') != pl.col('0031_Contract EXP Date'))
        .then(pl.lit('End Date not Match'))
        .otherwise(pl.lit('Exception'))
        .alias('Contract Header Check')
    )

    df = df.with_columns(pl.lit(None).alias('Suggested Contract No.'))

    return df


def _add_reference_mappings(df: pl.DataFrame, config: dict, paths: dict) -> pl.DataFrame:
    """Add MFN and VN reference mappings."""
    print('\n--- Adding Reference Mappings ---')

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

    return df

def _add_highest_uom_price(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a column 'Highest_UOM_Price' to the dataframe based on UOM conversions.
    Handles string columns by casting to numeric types.
    """
    print('Adding highest_uom_price...')
    # Step 1: Cast quantity and price columns to numeric (Float64)
    df = df.with_columns([
        pl.col('AUOM1 QTY').str.replace_all(',','').cast(pl.Float64, strict=False),
        pl.col('AUOM2 QTY').str.replace_all(',','').cast(pl.Float64, strict=False),
        pl.col('AUOM3 QTY').str.replace_all(',','').cast(pl.Float64, strict=False),
        pl.col('Purchase UOM Price').str.replace_all(',','').cast(pl.Float64, strict=False)
    ])
    
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
    price_per_base = pl.col('Purchase UOM Price') / purchase_qty
    
    # Step 4: Calculate prices for each UOM level
    base_price = price_per_base
    auom1_price = price_per_base * pl.col('AUOM1 QTY').fill_null(0.0)
    auom2_price = price_per_base * pl.col('AUOM2 QTY').fill_null(0.0)
    auom3_price = price_per_base * pl.col('AUOM3 QTY').fill_null(0.0)
    
    # Step 5: Find the maximum price across all UOMs
    df = df.with_columns([
        pl.max_horizontal([
            base_price,
            auom1_price,
            auom2_price,
            auom3_price
        ]).alias('Highest_UOM_Price')
    ])
    
    return df



def _finalize_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """Apply final transformations to the DataFrame."""
    print('\n--- Finalizing DataFrame ---')

    # Rename for compatibility
    df = df.rename({'PMM': '0031_PMM Item Number'})

    # Add duplicates flag
    df = df.with_columns(
        [pl.when(pl.len().over(['Source_File', 'Index']) > 1).then(pl.lit('Y')).otherwise(pl.lit('')).alias('Duplicates')]
    )

    return df


def _save_integrated_output(df: pl.DataFrame, config: dict, paths: dict) -> Path:
    """Save integrated output to parquet."""
    from datetime import datetime

    output_folder = paths['integrated_output']
    output_folder.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_folder / f'integrated_{timestamp}.parquet'

    df.write_parquet(output_file)
    print(f'Saved integrated output: {output_file.name}')

    return output_file


def get_integrate_status(config: dict, paths: dict) -> dict:
    """Get Phase 1 (Integration) status"""
    integrated_output = paths['integrated_output']

    if not integrated_output.exists():
        return {'phase': 'Integration', 'status': 'Not started', 'output_files': 0}

    output_files = list(integrated_output.glob('*.parquet'))

    return {
        'phase': 'Integration',
        'status': 'Completed' if output_files else 'Not started',
        'output_files': len(output_files),
        'latest_output': output_files[-1].name if output_files else None,
    }


__all__ = ['get_integrate_status', 'process_integrate']
