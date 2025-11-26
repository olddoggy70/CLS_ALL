"""
Phase 1: Integration
Integrate daily files with 0031 baseline database
"""

import logging
from pathlib import Path

import polars as pl

from ..constants import Columns0031, DailyColumns
from ..utils.date_utils import extract_date_range

# Import utilities
from ..utils.file_operations import archive_file

# Import new modules
from . import baseline, enrichment, ingest


def process_integrate(config: dict, paths: dict, logger: logging.Logger | None = None) -> bool:
    """
    Phase 1: Integrate daily files with 0031 baseline

    Args:
        config: Configuration dict
        paths: Paths dict
        logger: Logger instance for output

    Returns:
        True if processing succeeded
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    try:
        # Process daily data
        daily_df = ingest.process_daily_data(config, paths, logger)

        # Prepare database DataFrame
        db_df = baseline.prepare_database_dataframe(config, paths, logger)

        # Create lookup tables
        lookup_tables = baseline.create_lookup_tables(db_df, logger)

        # Enrich daily data
        enriched_df = enrichment.enrich_daily_data(daily_df, lookup_tables, logger)

        # Add contract analysis
        enriched_df = enrichment.add_contract_analysis(enriched_df, lookup_tables[3], logger)

        # Add reference mappings
        enriched_df = enrichment.add_reference_mappings(enriched_df, config, paths, logger)

        # Add highest UoM Price
        enriched_df = enrichment.add_highest_uom_price(enriched_df, logger)

        # Finalize DataFrame
        final_df = _finalize_dataframe(enriched_df, logger)

        # Save integrated output
        output_path = _save_integrated_output(final_df, config, paths, logger)

        logger.info(f'✓ Phase 1 completed successfully')
        logger.info(f'  Output: {output_path.name}')
        logger.info(f'  Rows: {len(final_df):,}, Columns: {len(final_df.columns)}')

        # Archive processed daily files
        _archive_daily_files(config, paths, logger)

        return True

    except Exception as e:
        logger.error(f'Phase 1 failed: {e}')
        import traceback

        logger.debug(traceback.format_exc())
        raise


def _finalize_dataframe(df: pl.DataFrame, logger: logging.Logger | None = None) -> pl.DataFrame:
    """Apply final transformations to the DataFrame."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    logger.debug('Finalizing DataFrame...')

    # Rename for compatibility
    df = df.rename({DailyColumns.PMM: f'0031_{Columns0031.PMM_ITEM_NUMBER}'})

    # Add duplicates flag
    df = df.with_columns(
        [
            pl.when(pl.len().over([DailyColumns.SOURCE_FILE, Columns0031.INDEX]) > 1)
            .then(pl.lit('Y'))
            .otherwise(pl.lit(''))
            .alias('Duplicates')
        ]
    )

    return df


def _save_integrated_output(df: pl.DataFrame, config: dict, paths: dict, logger: logging.Logger | None = None) -> Path:
    """Save integrated output with smart filename and configurable format."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    output_folder = paths['integrated_output']
    output_folder.mkdir(parents=True, exist_ok=True)

    # Get output settings from config
    integration_config = config.get('phases', {}).get('integration', {})
    output_format = integration_config.get('output_format', 'parquet')
    date_column = integration_config.get('date_column', Columns0031.DATE_AND_TIME)
    date_format = integration_config.get('date_format', '%Y-%b-%d %I:%M:%S %p')
    filename_prefix = integration_config.get('filename_prefix', 'integrated')

    # Extract date range for filename
    date_range = extract_date_range(df, date_column, date_format, logger)

    # Generate filename
    filename = f'{filename_prefix}_{date_range}.{output_format}'
    output_file = output_folder / filename

    # Save based on format
    if output_format == 'parquet':
        df.write_parquet(output_file)
    elif output_format == 'xlsx':
        df.write_excel(output_file, autofit=True)
    elif output_format == 'csv':
        df.write_csv(output_file)
    else:
        logger.warning(f'Unknown format "{output_format}", defaulting to parquet')
        output_file = output_folder / f'{filename_prefix}_{date_range}.parquet'
        df.write_parquet(output_file)

    logger.debug(f'Saved: {output_file.name}')

    return output_file


def _archive_daily_files(config: dict, paths: dict, logger: logging.Logger | None = None):
    """Archive processed daily files after successful integration."""
    if logger is None:
        logger = logging.getLogger('data_pipeline.integrate')

    daily_files_folder = paths['daily_files_folder']
    daily_archive_folder = paths['daily_archive_folder']

    # Check if archiving is enabled in config
    archive_enabled = config.get('file_patterns', {}).get('daily_files', {}).get('archive_after_processing', True)

    if not archive_enabled:
        logger.debug('Daily file archiving disabled in config')
        return

    # Get all daily files
    daily_files = list(daily_files_folder.glob('*.xlsx'))

    if not daily_files:
        logger.debug('No daily files to archive')
        return

    archived_count = 0
    for daily_file in daily_files:
        try:
            archive_file(daily_file, daily_archive_folder, logger)
            archived_count += 1
        except Exception as e:
            logger.warning(f'Failed to archive {daily_file.name}: {e}')

    if archived_count > 0:
        logger.info(f'  Archived {archived_count} daily file(s)')


def get_integrate_status(config: dict, paths: dict) -> dict:
    """Get Phase 1 (Integration) status"""
    integrated_output = paths['integrated_output']

    if not integrated_output.exists():
        return {'phase': 'Integration', 'status': 'Not started', 'output_files': 0}

    # Get output format from config, default to parquet
    output_format = config.get('phases', {}).get('integration', {}).get('output_format', 'parquet')

    # Check for files with the configured extension
    output_files = list(integrated_output.glob(f'*.{output_format}'))

    # Sort by modification time (newest last)
    output_files.sort(key=lambda f: f.stat().st_mtime)

    return {
        'phase': 'Integration',
        'status': 'Completed' if output_files else 'Not started',
        'output_files': len(output_files),
        'latest_output': output_files[-1].name if output_files else None,
    }


__all__ = ['get_integrate_status', 'process_integrate']
