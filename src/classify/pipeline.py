"""
Phase 2: Classification
Classify records into buckets (update, create, vendor link, contract link)
"""

from datetime import datetime
from pathlib import Path

import polars as pl


def process_classify(config: dict, paths: dict, logger=None) -> bool:
    """
    Phase 2: Classify records into buckets

    Args:
        config: Configuration dict
        paths: Paths dict
        logger: Logger instance

    Returns:
        True if processing succeeded
    """
    if logger is None:
        import logging
        logger = logging.getLogger('data_pipeline.classify')

    logger.info('=' * 60)
    logger.info('PHASE 2: CLASSIFICATION')
    logger.info('Classify records into buckets')
    logger.info('=' * 60)

    integrated_output = paths['integrated_output']
    classified_output = paths['classified_output']

    # Ensure output folder exists
    classified_output.mkdir(parents=True, exist_ok=True)

    # Get integrated files
    integrated_files = list(integrated_output.glob('*.parquet'))

    if not integrated_files:
        logger.warning('⚠ No integrated files found')
        logger.warning(f'  Expected in: {integrated_output}')
        logger.warning('  Run "python main.py integrate" first')
        return False

    logger.info(f'Found {len(integrated_files)} integrated file(s)')

    for integrated_file in integrated_files:
        logger.info(f'Processing: {integrated_file.name}')

        try:
            df = pl.read_parquet(integrated_file)
            logger.debug(f'  Rows: {len(df):,}')

            # Classification Logic
            # Bucket: 'update' if 0031_PMM Item Number exists, else 'create'
            # TODO: Implement logic for 'vendor_link' and 'contract_link' buckets when rules are defined

            if '0031_PMM Item Number' in df.columns:
                df = df.with_columns(
                    pl.when(pl.col('0031_PMM Item Number').is_not_null())
                    .then(pl.lit('update'))
                    .otherwise(pl.lit('create'))
                    .alias('Bucket')
                )
            else:
                # Fallback if column missing (shouldn't happen if integrated correctly)
                logger.warning('  "0031_PMM Item Number" column missing, defaulting to "create"')
                df = df.with_columns(pl.lit('create').alias('Bucket'))

            # Calculate stats
            bucket_counts = df.group_by('Bucket').count()
            for row in bucket_counts.iter_rows(named=True):
                logger.info(f'  Bucket "{row["Bucket"]}": {row["count"]:,} records')

            # Save classified file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = classified_output / f'classified_{timestamp}.parquet'
            df.write_parquet(output_file)
            logger.info(f'  ✓ Saved: {output_file.name}')

        except Exception as e:
            logger.error(f'  Failed to process {integrated_file.name}: {e}')
            return False

    logger.info('✓ Phase 2 completed successfully')
    return True


def get_classify_status(config: dict, paths: dict) -> dict:
    """Get Phase 2 (Classification) status"""
    classified_output = paths['classified_output']

    if not classified_output.exists():
        return {'phase': 'Classification', 'status': 'Not started', 'buckets': 0, 'total_files': 0}

    classified_files = list(classified_output.glob('*.parquet'))
    
    buckets = 0
    if classified_files:
        try:
            # Check the latest file for buckets
            latest_file = max(classified_files, key=lambda p: p.stat().st_mtime)
            df = pl.read_parquet(latest_file)
            if 'Bucket' in df.columns:
                buckets = df['Bucket'].n_unique()
        except Exception:
            pass

    return {
        'phase': 'Classification',
        'status': 'Completed' if classified_files else 'Not started',
        'buckets': buckets,
        'total_files': len(classified_files),
    }


__all__ = ['get_classify_status', 'process_classify']
