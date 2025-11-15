"""
Phase 3: Export
Generate export files from classified data
"""

from datetime import datetime
from pathlib import Path

import polars as pl


def process_export(config: dict, paths: dict) -> bool:
    """
    Phase 3: Generate export files

    Args:
        config: Configuration dict
        paths: Paths dict

    Returns:
        True if processing succeeded
    """
    print('\n' + '=' * 60)
    print('PHASE 3: EXPORT')
    print('Generate export files')
    print('=' * 60)

    # For now, use integrated output directly (until classification is implemented)
    # TODO: Change to classified_output when Phase 2 is implemented
    input_folder = paths['integrated_output']
    exports_output = paths['exports_output']

    # Ensure output folder exists
    exports_output.mkdir(parents=True, exist_ok=True)

    # Get input files
    input_files = list(input_folder.glob('*.parquet'))

    if not input_files:
        print('⚠ No input files found')
        print(f'  Expected in: {input_folder}')
        print('  Run "python main.py integrate" first')
        return False

    print(f'Found {len(input_files)} input file(s)')

    # Get the latest file
    latest_file = max(input_files, key=lambda p: p.stat().st_mtime)
    print(f'\nProcessing latest: {latest_file.name}')

    df = pl.read_parquet(latest_file)
    print(f'  Rows: {len(df):,}')

    # Generate export filename with date range
    export_filename = _generate_export_filename(df)
    export_path = exports_output / export_filename

    # Export to Excel
    print(f'\nExporting to: {export_filename}')
    df.write_excel(str(export_path), autofit=True)

    print(f'\n✓ Phase 3 completed successfully')
    print(f'  Export file: {export_path}')

    return True


def _generate_export_filename(df: pl.DataFrame) -> str:
    """Generate export filename with date range from data"""

    # Extract unique dates from Date and Time Stamp column
    if 'Date and Time Stamp' not in df.columns:
        # Fallback to timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f'ExportFile_{timestamp}.xlsx'

    try:
        dates = (
            df.select(
                pl.col('Date and Time Stamp')
                .str.strptime(pl.Datetime, '%Y-%b-%d %I:%M:%S %p', strict=False)
                .dt.date()
                .dt.strftime('%Y-%m-%d')
                .drop_nulls()
            )
            .unique()
            .sort(by=pl.col('Date and Time Stamp'))
            .to_series()
            .to_list()
        )

        if len(dates) == 0:
            export_date = 'no_data'
        elif len(dates) == 1:
            export_date = dates[0]
        else:
            # Multiple dates: format as "start_date~end_date"
            export_date = f'{dates[0]}~{dates[-1]}'

        print(f'Export date range: {export_date}')
        return f'ExportFile_{export_date}.xlsx'

    except Exception as e:
        print(f'Warning: Could not parse dates: {e}')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f'ExportFile_{timestamp}.xlsx'


def get_export_status(config: dict, paths: dict) -> dict:
    """Get Phase 3 (Export) status"""
    exports_output = paths['exports_output']

    if not exports_output.exists():
        return {'phase': 'Export', 'status': 'Not started', 'export_files': 0}

    export_files = list(exports_output.glob('ExportFile_*.xlsx'))

    return {
        'phase': 'Export',
        'status': 'Completed' if export_files else 'Not started',
        'export_files': len(export_files),
        'latest_export': export_files[-1].name if export_files else None,
    }


__all__ = ['get_export_status', 'process_export']
