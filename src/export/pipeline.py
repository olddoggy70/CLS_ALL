"""
Phase 3: Export
Generate export files from classified data
"""

from datetime import datetime
from pathlib import Path

import polars as pl


def process_export(config: dict, paths: dict, logger=None) -> bool:
    """
    Phase 3: Generate export files

    Args:
        config: Configuration dict
        paths: Paths dict
        logger: Logger instance

    Returns:
        True if processing succeeded
    """
    if logger is None:
        import logging
        logger = logging.getLogger('data_pipeline.export')

    logger.info('=' * 60)
    logger.info('PHASE 3: EXPORT')
    logger.info('Generate export files')
    logger.info('=' * 60)

    classified_output = paths['classified_output']
    exports_output = paths['exports_output']

    # Ensure output folder exists
    exports_output.mkdir(parents=True, exist_ok=True)

    # Get input files
    input_files = list(classified_output.glob('*.parquet'))

    if not input_files:
        logger.warning('⚠ No classified files found')
        logger.warning(f'  Expected in: {classified_output}')
        logger.warning('  Run "python main.py classify" first')
        return False

    logger.info(f'Found {len(input_files)} classified file(s)')

    # Get the latest file
    latest_file = max(input_files, key=lambda p: p.stat().st_mtime)
    logger.info(f'Processing latest: {latest_file.name}')

    try:
        df = pl.read_parquet(latest_file)
        logger.debug(f'  Rows: {len(df):,}')

        # Generate export filename
        export_filename = _generate_export_filename(df)
        export_path = exports_output / export_filename

        logger.info(f'Exporting to: {export_filename}')

        # Write to Excel with multiple sheets
        # 1. All Data
        # 2. Each Bucket
        
        # We need to use xlsxwriter or openpyxl via polars
        # Polars write_excel supports workbook options
        
        with getattr(pl.Config, "context", lambda: None)(): # Context manager for config if needed, but not needed for write_excel
             pass

        # Using xlsxwriter engine for better control if needed, but default is fine
        # We will write separate sheets
        
        # Get buckets
        if 'Bucket' in df.columns:
            buckets = df['Bucket'].unique().to_list()
            buckets.sort()
        else:
            buckets = []
            logger.warning('  "Bucket" column missing in classified data')

        # Write Excel
        # Note: Polars write_excel writes a single dataframe. To write multiple sheets, we need a workbook object or multiple calls?
        # Polars write_excel writes the whole file.
        # To write multiple sheets, we need to use a library like xlsxwriter directly or use polars with a workbook.
        # Actually, polars `write_excel` creates a new file.
        
        # Let's use xlsxwriter directly for multi-sheet support, converting from polars
        import xlsxwriter
        
        workbook = xlsxwriter.Workbook(str(export_path))
        
        # Helper to write sheet
        def write_sheet(dataframe, sheet_name):
            worksheet = workbook.add_worksheet(sheet_name)
            
            # Write headers
            for col_num, value in enumerate(dataframe.columns):
                worksheet.write(0, col_num, value)
            
            # Write data
            # Converting to rows is expensive for large data, but safest for xlsxwriter
            # For better performance, we could use polars `write_excel` if it supported appending sheets, but it doesn't easily.
            # Or we can use `df.write_excel(workbook=wb, worksheet=ws)` if supported? No.
            
            # Let's try to use pandas if available, as it handles multi-sheet well?
            # Or just iterate.
            
            rows = dataframe.rows()
            for row_num, row_data in enumerate(rows, 1):
                for col_num, value in enumerate(row_data):
                    # Handle None/NaN
                    if value is None:
                        continue
                    
                    # Handle dates
                    if isinstance(value, (datetime,)):
                        # Format? Xlsxwriter handles datetime objects
                        pass
                        
                    worksheet.write(row_num, col_num, value)
                    
        # Check if data is too large for simple iteration. If > 100k rows, this might be slow.
        # If so, maybe just write 'All Data' using polars and skip buckets?
        # Or use pandas `ExcelWriter` if pandas is installed.
        # Let's check if pandas is available.
        
        try:
            import pandas as pd
            # Convert to pandas and write
            logger.info('  Using pandas for Excel export (multi-sheet)...')
            
            with pd.ExcelWriter(str(export_path), engine='xlsxwriter') as writer:
                # All Data
                df.to_pandas().to_excel(writer, sheet_name='All Data', index=False)
                
                # Buckets
                for bucket in buckets:
                    bucket_df = df.filter(pl.col('Bucket') == bucket)
                    if len(bucket_df) > 0:
                        bucket_df.to_pandas().to_excel(writer, sheet_name=f'Bucket - {bucket}', index=False)
                        
        except ImportError:
            logger.info('  Pandas not found, falling back to single sheet export via Polars')
            df.write_excel(str(export_path), autofit=True)

        logger.info(f'✓ Phase 3 completed successfully')
        logger.info(f'  Export file: {export_path}')

        return True

    except Exception as e:
        logger.error(f'Phase 3 (Export) failed: {e}')
        import traceback
        logger.debug(traceback.format_exc())
        return False


def _generate_export_filename(df: pl.DataFrame) -> str:
    """Generate export filename with date range from data"""

    # Extract unique dates from Date and Time Stamp column
    # Note: In Phase 1 we might have renamed columns or it might be 'Date and Time Stamp' from source
    # Let's check for 'Date and Time Stamp' or 'Item Update Date'
    
    date_col = None
    if 'Date and Time Stamp' in df.columns:
        date_col = 'Date and Time Stamp'
    elif 'Item Update Date' in df.columns:
        date_col = 'Item Update Date'
        
    if not date_col:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f'ExportFile_{timestamp}.xlsx'

    try:
        # Try to parse dates
        # If it's already date type
        if df[date_col].dtype in [pl.Date, pl.Datetime]:
             dates = df[date_col].dt.date().unique().sort().to_list()
        else:
            # Try parsing string
            dates = (
                df.select(
                    pl.col(date_col)
                    .str.to_date('%Y-%m-%d', strict=False) # Try common format
                    .drop_nulls()
                )
                .unique()
                .sort(by=date_col)
                .to_series()
                .to_list()
            )

        if len(dates) == 0:
            export_date = 'no_data'
        elif len(dates) == 1:
            export_date = str(dates[0])
        else:
            # Multiple dates: format as "start_date~end_date"
            export_date = f'{dates[0]}~{dates[-1]}'

        # Clean filename characters
        export_date = export_date.replace('/', '-')
        
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
