"""
Data quality operations - validation and change tracking
"""

import logging
from pathlib import Path

import polars as pl

from ..constants import Columns0031


def validate_parquet_data(
    df: pl.DataFrame, blank_vpn_permitted_file: Path | None = None, logger: logging.Logger | None = None
) -> dict:
    """
    Validate parquet data for quality issues

    Args:
        df: DataFrame to validate
        blank_vpn_permitted_file: Path to Excel file with PMM Item Numbers allowed to have blank Vendor Catalogue
        logger: Logger instance

    Returns dictionary with validation results:
    - contracts_with_multiple_vendors: List of contracts with multiple vendor codes
    - contracts_with_multiple_vendors_df: DataFrame with full details
    - blank_vendor_catalogue_count: Count of records with blank Vendor Catalogue (excluding permitted)
    - blank_vendor_catalogue_df: DataFrame with full details
    - inconsistent_vendor_catalogue_count: Count of PMM-Vendor-CorpAcct combinations with multiple catalogue values
    - inconsistent_vendor_catalogue_df: DataFrame with full details
    - inconsistent_vendor_catalogue_items: List of dicts for reporting
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    logger.info('=== Validating Data Quality ===')
    validation_results = {
        'has_issues': False,
        'contracts_with_multiple_vendors': [],
        'contracts_with_multiple_vendors_df': None,
        'blank_vendor_catalogue_count': 0,
        'blank_vendor_catalogue_df': None,
        'inconsistent_vendor_catalogue_count': 0,
        'inconsistent_vendor_catalogue_df': None,
        'inconsistent_vendor_catalogue_items': [],
    }

    # Load permitted blank VPN list if provided
    permitted_pmm_list = set()
    if blank_vpn_permitted_file and blank_vpn_permitted_file.exists():
        try:
            permitted_df = pl.read_excel(blank_vpn_permitted_file, infer_schema_length=0)
            if Columns0031.PMM_ITEM_NUMBER in permitted_df.columns:
                permitted_pmm_list = set(permitted_df.select(Columns0031.PMM_ITEM_NUMBER).to_series().to_list())
                logger.debug(f'Loaded {len(permitted_pmm_list)} permitted PMM Item Numbers for blank Vendor Catalogue')
        except Exception as e:
            logger.warning(f'Could not load permitted blank VPN file: {e}')

    # Check 1: Contract No should have 1-to-1 relationship with Vendor Code
    if 'Contract No' in df.columns and Columns0031.VENDOR_CODE in df.columns:
        logger.debug('Checking Contract No to Vendor Code relationship...')

        contract_vendor_counts = (
            df.filter(
                pl.col('Contract No').is_not_null()
                & pl.col(Columns0031.VENDOR_CODE).is_not_null()
                & (pl.col('Contract No').cast(pl.Utf8).str.strip_chars() != 'N/A')
            )
            .group_by('Contract No')
            .agg([pl.col(Columns0031.VENDOR_CODE).n_unique().alias('vendor_count'), pl.col(Columns0031.VENDOR_CODE).unique().alias('vendor_codes')])
            .filter(pl.col('vendor_count') > 1)
        )

        if len(contract_vendor_counts) > 0:
            validation_results['has_issues'] = True
            validation_results['contracts_with_multiple_vendors'] = contract_vendor_counts.to_dicts()
            validation_results['contracts_with_multiple_vendors_df'] = contract_vendor_counts
            logger.warning(f'WARNING: Found {len(contract_vendor_counts)} contract(s) with multiple vendors')
        else:
            logger.debug('  ✓ Contract-Vendor relationship check passed')
    else:
        logger.warning('Cannot validate Contract-Vendor relationship: Required columns not found')

    # Check 2: Find records with blank Vendor Catalogue (excluding permitted PMM Item Numbers)
    if 'Vendor Catalogue' in df.columns:
        logger.debug('Checking for blank Vendor Catalogue values...')

        blank_catalogue = df.filter(
            pl.col('Vendor Catalogue').is_null() | (pl.col('Vendor Catalogue').cast(pl.Utf8).str.strip_chars() == '')
        )

        if permitted_pmm_list:
            blank_catalogue = blank_catalogue.filter(~pl.col(Columns0031.PMM_ITEM_NUMBER).is_in(list(permitted_pmm_list)))

        blank_count = len(blank_catalogue)
        validation_results['blank_vendor_catalogue_count'] = blank_count

        if blank_count > 0:
            validation_results['has_issues'] = True
            validation_results['blank_vendor_catalogue_df'] = blank_catalogue
            logger.warning(f'WARNING: Found {blank_count} record(s) with blank Vendor Catalogue')
            if permitted_pmm_list:
                logger.debug(f'      (Excluded {len(permitted_pmm_list)} permitted PMM Item Numbers)')
        else:
            logger.debug('  ✓ Blank Vendor Catalogue check passed')
            if permitted_pmm_list:
                logger.debug(f'    ({len(permitted_pmm_list)} PMM Item Numbers permitted to have blank Vendor Catalogue)')
    else:
        logger.warning('Cannot check Vendor Catalogue: Column not found')

    # Check 3: Vendor Catalogue consistency check (now includes first 2 chars of Corp Acct)
    if all(col in df.columns for col in [Columns0031.PMM_ITEM_NUMBER, Columns0031.VENDOR_CODE, 'Vendor Catalogue', Columns0031.CORP_ACCT]):
        logger.debug('Checking Vendor Catalogue consistency (PMM Item Number + Vendor Code + Corp Acct prefix)...')

        consistency_df = df.filter(
            pl.col(Columns0031.PMM_ITEM_NUMBER).is_not_null()
            & pl.col(Columns0031.VENDOR_CODE).is_not_null()
            & pl.col('Vendor Catalogue').is_not_null()
            & pl.col(Columns0031.CORP_ACCT).is_not_null()
            & pl.col('Vendor Seq').is_not_null()
            & (pl.col('Vendor Catalogue').cast(pl.Utf8).str.strip_chars() != '')
        ).with_columns(pl.col(Columns0031.CORP_ACCT).cast(pl.Utf8).str.slice(0, 2).alias('Corp_Acct_Prefix'))

        catalogue_consistency = (
            consistency_df.group_by([Columns0031.PMM_ITEM_NUMBER, Columns0031.VENDOR_CODE, 'Corp_Acct_Prefix'])
            .agg(
                [
                    pl.col('Vendor Catalogue').n_unique().alias('catalogue_count'),
                    pl.col('Vendor Catalogue').unique().alias('catalogue_values'),
                    pl.col('Vendor Seq').unique().alias('vendor_seq_values'),
                ]
            )
            .filter(pl.col('catalogue_count') > 1)
        )

        inconsistent_count = len(catalogue_consistency)
        validation_results['inconsistent_vendor_catalogue_count'] = inconsistent_count

        if inconsistent_count > 0:
            validation_results['has_issues'] = True
            validation_results['inconsistent_vendor_catalogue_df'] = catalogue_consistency

            # Convert to list of dicts for reporting
            validation_results['inconsistent_vendor_catalogue_items'] = [
                {
                    'pmm_item': row[Columns0031.PMM_ITEM_NUMBER],
                    'vendor_code': row[Columns0031.VENDOR_CODE],
                    'vendor_seq': ', '.join(str(v) for v in row['vendor_seq_values']) if row['vendor_seq_values'] else '',
                    'corp_acct': row['Corp_Acct_Prefix'],
                    'unique_catalogues': row['catalogue_count'],
                    'catalogue_values': ', '.join(str(v) for v in row['catalogue_values']) if row['catalogue_values'] else '',
                }
                for row in catalogue_consistency.iter_rows(named=True)
            ]

            logger.warning(f'WARNING: Found {inconsistent_count} PMM-Vendor-CorpAcct combination(s) with inconsistent catalogues')
        else:
            logger.debug('  ✓ Vendor Catalogue consistency check passed')
    else:
        logger.warning('Cannot check Vendor Catalogue consistency: Required columns not found')

    if not validation_results['has_issues']:
        logger.info('✓ All validation checks passed!')

    return validation_results


def track_row_changes(
    current_df: pl.DataFrame, previous_df: pl.DataFrame, audit_folder: Path, logger: logging.Logger | None = None
) -> dict:
    """
    Compare current and previous dataframes to identify changed rows
    Uses 5-column unique key: PMM Item Number, Corp Acct, Vendor Code, Additional Cost Centre, Additional GL Account

    FIXED: Now processes ALL rows in current_df (not just latest date)
    Supports files with single date OR multiple dates

    Args:
        current_df: Current DataFrame from Excel files (incremental data)
        previous_df: Previous DataFrame from parquet backup (existing database)
        audit_folder: Folder to save audit files
        logger: Logger instance

    Returns:
        Dictionary with change summary and full dataframes, including date breakdown
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    logger.debug('=== Tracking Row Changes ===')

    # Check for required columns (5-column key)
    unique_keys = [Columns0031.PMM_ITEM_NUMBER, Columns0031.CORP_ACCT, Columns0031.VENDOR_CODE, Columns0031.ADD_COST_CENTRE, Columns0031.ADD_GL_ACCOUNT]
    date_col = Columns0031.ITEM_UPDATE_DATE

    missing_keys = [col for col in unique_keys if col not in current_df.columns]
    if missing_keys:
        logger.warning(f'Cannot track changes - missing columns: {missing_keys}')
        return {
            'has_changes': False,
            'changes_summary': None,
            'changes_df': None,
            'new_rows_df': None,
            'updated_rows_df': None,
            'date_breakdown': None,
        }

    if date_col not in current_df.columns:
        logger.warning(f'Cannot track changes - missing column: {date_col}')
        return {
            'has_changes': False,
            'changes_summary': None,
            'changes_df': None,
            'new_rows_df': None,
            'updated_rows_df': None,
            'date_breakdown': None,
        }

    # Create date breakdown for all dates in current_df
    date_breakdown = current_df.group_by(date_col).agg(pl.count().alias('row_count')).sort(date_col)

    # Get latest date for reporting
    latest_date = current_df.select(pl.col(date_col)).max().item()
    logger.debug(f'Latest Item Update Date: {latest_date}')
    logger.debug(f'Total rows in incremental data: {len(current_df):,}')

    # Show date distribution
    if len(date_breakdown) > 1:
        logger.debug(f'Date range: {len(date_breakdown)} unique dates')
        for row in date_breakdown.iter_rows(named=True):
            logger.debug(f'  {row[Columns0031.ITEM_UPDATE_DATE]}: {row["row_count"]:,} rows')

    # FIXED: Process ALL rows in current_df, not just latest date
    if len(current_df) == 0:
        logger.debug('  No rows found in incremental data')
        return {
            'has_changes': False,
            'changes_summary': None,
            'changes_df': None,
            'new_rows_df': None,
            'updated_rows_df': None,
            'date_breakdown': date_breakdown,
        }

    # Create unique key column for joining (5-column key)
    current_with_key = current_df.with_columns(
        pl.concat_str(
            [
                pl.col('PMM Item Number').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Corp Acct').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Vendor Code').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional Cost Centre').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional GL Account').cast(pl.Utf8).fill_null('').str.strip_chars(),
            ],
            separator='|',
        ).alias('_unique_key')
    )

    previous_with_key = previous_df.with_columns(
        pl.concat_str(
            [
                pl.col('PMM Item Number').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Corp Acct').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Vendor Code').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional Cost Centre').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional GL Account').cast(pl.Utf8).fill_null('').str.strip_chars(),
            ],
            separator='|',
        ).alias('_unique_key')
    )

    # Identify new rows (not in previous data) vs updated rows (in previous data)
    current_keys = set(current_with_key.select('_unique_key').to_series().to_list())
    previous_keys = set(previous_with_key.select('_unique_key').to_series().to_list())

    new_keys = current_keys - previous_keys
    updated_keys = current_keys & previous_keys

    new_rows = current_with_key.filter(pl.col('_unique_key').is_in(list(new_keys)))
    updated_rows_current = current_with_key.filter(pl.col('_unique_key').is_in(list(updated_keys)))

    # logger.debug(f' new_keys: {new_keys}')
    logger.debug(f'  New rows: {len(new_rows):,}')
    logger.debug(f'  Updated rows: {len(updated_rows_current):,}')

    # Build change audit dataframe
    # Build change audit dataframe
    changes_list = []

    # For updated rows, compare with previous values using vectorized operations
    if len(updated_rows_current) > 0:
        updated_rows_previous = previous_with_key.filter(pl.col('_unique_key').is_in(list(updated_keys)))

        # Identify non-key columns to compare
        compare_cols = list(set(current_with_key.columns) - {'_unique_key', date_col, 'source_file', '_merge_key'})

        # Select only necessary columns + key
        curr_subset = updated_rows_current.select(['_unique_key', date_col, *compare_cols])
        prev_subset = updated_rows_previous.select(['_unique_key', *compare_cols])

        # Melt both to long format: _unique_key, variable (Column), value
        curr_long = curr_subset.melt(id_vars=['_unique_key', date_col], value_vars=compare_cols, variable_name='Column', value_name='Current Value')
        prev_long = prev_subset.melt(id_vars=['_unique_key'], value_vars=compare_cols, variable_name='Column', value_name='Previous Value')

        # Join on Key + Column
        joined_long = curr_long.join(prev_long, on=['_unique_key', 'Column'], how='inner')

        # Filter for changes
        # Handle nulls: if one is null and other isn't, or if values differ
        # We cast to string and treat empty strings as nulls, but we DO NOT strip whitespace
        # This ensures 'ABC' vs 'AB C' is treated as a change
        changes_df_updates = joined_long.filter(
            pl.col('Current Value').cast(pl.Utf8).fill_null('') != pl.col('Previous Value').cast(pl.Utf8).fill_null('')
        )

        if len(changes_df_updates) > 0:
            # Parse the unique key back into component columns
            # Key format: PMM|Corp|Vendor|Cost|GL
            changes_df_updates = changes_df_updates.with_columns(
                pl.col('_unique_key').str.split('|').alias('key_parts')
            ).with_columns(
                [
                    pl.col('key_parts').list.get(0).alias(Columns0031.PMM_ITEM_NUMBER),
                    pl.col('key_parts').list.get(1).alias(Columns0031.CORP_ACCT),
                    pl.col('key_parts').list.get(2).alias(Columns0031.VENDOR_CODE),
                    pl.col('key_parts').list.get(3).alias(Columns0031.ADD_COST_CENTRE),
                    pl.col('key_parts').list.get(4).alias(Columns0031.ADD_GL_ACCOUNT),
                    pl.lit('Updated').alias('Change Type')
                ]
            ).drop(['key_parts', '_unique_key'])

            # Cast values to string for consistency
            changes_df_updates = changes_df_updates.with_columns([
                pl.col('Current Value').cast(pl.Utf8),
                pl.col('Previous Value').cast(pl.Utf8),
                pl.col(Columns0031.ITEM_UPDATE_DATE).cast(pl.Utf8)
            ])

    # Add new rows to changes
    # For new rows, we also want to record them in the audit log (optional, but good for completeness)
    # The original code added them, so we will too
    changes_df_new = None
    if len(new_rows) > 0:
        # Identify columns
        new_cols = list(set(new_rows.columns) - {'_unique_key', date_col})

        # Melt
        new_long = new_rows.select(['_unique_key', date_col, *new_cols]).melt(
            id_vars=['_unique_key', date_col],
            value_vars=new_cols,
            variable_name='Column',
            value_name='Current Value'
        )

        # Filter out nulls (optional, but usually we only care about populated fields)
        new_long = new_long.filter(pl.col('Current Value').is_not_null())

        if len(new_long) > 0:
             changes_df_new = new_long.with_columns(
                pl.col('_unique_key').str.split('|').alias('key_parts')
            ).with_columns(
                [
                    pl.col('key_parts').list.get(0).alias(Columns0031.PMM_ITEM_NUMBER),
                    pl.col('key_parts').list.get(1).alias(Columns0031.CORP_ACCT),
                    pl.col('key_parts').list.get(2).alias(Columns0031.VENDOR_CODE),
                    pl.col('key_parts').list.get(3).alias(Columns0031.ADD_COST_CENTRE),
                    pl.col('key_parts').list.get(4).alias(Columns0031.ADD_GL_ACCOUNT),
                    pl.lit(None).cast(pl.Utf8).alias('Previous Value'),
                    pl.lit('New').alias('Change Type')
                ]
            ).drop(['key_parts', '_unique_key'])

             changes_df_new = changes_df_new.with_columns([
                pl.col('Current Value').cast(pl.Utf8),
                pl.col(Columns0031.ITEM_UPDATE_DATE).cast(pl.Utf8)
            ])

    # Combine updates and new
    audit_df = pl.DataFrame()
    if 'changes_df_updates' in locals() and len(changes_df_updates) > 0:
        audit_df = pl.concat([audit_df, changes_df_updates], how='diagonal')

    if changes_df_new is not None and len(changes_df_new) > 0:
        audit_df = pl.concat([audit_df, changes_df_new], how='diagonal')

    if len(audit_df) == 0:
        logger.debug('  No changes detected')
        return {
            'has_changes': False,
            'changes_summary': None,
            'changes_df': None,
            'new_rows_df': None,
            'updated_rows_df': None,
            'date_breakdown': date_breakdown,
        }

    # Create audit dataframe
    # schema = { ... } # Schema is now inferred or set during creation
    # audit_df = pl.DataFrame(changes_list, schema=schema) # Already created above

    # Create summary
    change_summary = {
        'total_changes': len(audit_df),
        'new_rows': len(new_rows),
        'updated_rows': len(updated_rows_current),
        'latest_update_date': str(latest_date),
        'changes_by_column': audit_df.group_by('Column').agg(pl.len().alias('count')).to_dicts(),
    }

    # Separate new and updated changes
    new_changes_df = audit_df.filter(pl.col('Change Type') == 'New')
    updated_changes_df = audit_df.filter(pl.col('Change Type') == 'Updated')

    logger.debug(f'  Total field-level changes tracked: {len(audit_df):,}')

    return {
        'has_changes': True,
        'changes_summary': change_summary,
        'changes_df': audit_df,
        'new_rows_df': new_changes_df,
        'updated_rows_df': updated_changes_df,
        'date_breakdown': date_breakdown,
    }


def print_change_summary(change_results: dict, logger: logging.Logger | None = None):
    """Print change summary to console (summary only)"""
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    if not change_results.get('has_changes'):
        logger.debug('  No changes detected')
        return

    summary = change_results.get('changes_summary', {})
    logger.info('=== Change Summary ===')
    logger.info(f'Total changes: {summary.get("total_changes", 0):,}')
    logger.info(f'New rows: {summary.get("new_rows", 0):,}')
    logger.info(f'Updated rows: {summary.get("updated_rows", 0):,}')
    logger.info(f'Latest update date: {summary.get("latest_update_date", "N/A")}')
