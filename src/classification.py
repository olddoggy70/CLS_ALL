"""
Phase 2: Classification
Classify records into buckets (update, create, vendor link, contract link)
"""

from datetime import datetime
from pathlib import Path

import polars as pl


def process_classify(config: dict, paths: dict) -> bool:
    """
    Phase 2: Classify records into buckets

    Args:
        config: Configuration dict
        paths: Paths dict

    Returns:
        True if processing succeeded
    """
    print('\n' + '=' * 60)
    print('PHASE 2: CLASSIFICATION')
    print('Classify records into buckets')
    print('=' * 60)

    integrated_output = paths['integrated_output']
    classified_output = paths['classified_output']

    # Ensure output folder exists
    classified_output.mkdir(parents=True, exist_ok=True)

    # Get integrated files
    integrated_files = list(integrated_output.glob('*.parquet'))

    if not integrated_files:
        print('⚠ No integrated files found')
        print(f'  Expected in: {integrated_output}')
        print('  Run "python main.py integrate" first')
        return False

    print(f'Found {len(integrated_files)} integrated file(s)')

    # TODO: Implement your classification logic
    # For now, just copy the structure

    for integrated_file in integrated_files:
        print(f'\nProcessing: {integrated_file.name}')

        df = pl.read_parquet(integrated_file)
        print(f'  Rows: {len(df):,}')

        # TODO: Add your bucket classification logic here
        # Example buckets from config:
        buckets_config = config.get('phases', {}).get('classification', {}).get('buckets', {})

        # Placeholder: Save as single bucket for now
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = classified_output / f'classified_{timestamp}.parquet'
        df.write_parquet(output_file)
        print(f'  ✓ Saved: {output_file.name}')

    print('\n✓ Phase 2 completed successfully')
    return True


def get_classify_status(config: dict, paths: dict) -> dict:
    """Get Phase 2 (Classification) status"""
    classified_output = paths['classified_output']

    if not classified_output.exists():
        return {'phase': 'Classification', 'status': 'Not started', 'buckets': 0, 'total_files': 0}

    classified_files = list(classified_output.glob('*.parquet'))

    return {
        'phase': 'Classification',
        'status': 'Completed' if classified_files else 'Not started',
        'buckets': 0,  # TODO: Calculate unique buckets
        'total_files': len(classified_files),
    }


__all__ = ['get_classify_status', 'process_classify']
