"""
Reporting functions for validation and change tracking
"""

import logging
import re
from datetime import datetime
from pathlib import Path

import polars as pl

from ..constants import Columns0031


def save_combined_report(
    validation_results: dict,
    change_results: dict,
    processing_time: float,
    audit_folder: Path,
    logger: logging.Logger | None = None,
) -> dict:
    """
    Save combined validation and change tracking report in both Markdown and Excel formats

    Args:
        validation_results: Dictionary containing validation check results
        change_results: Dictionary containing change tracking results (with per-file summaries)
        processing_time: Total processing time in seconds
        audit_folder: Folder to save reports
        logger: Logger instance

    Returns:
        Dictionary with paths to generated report files
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    timestamp = datetime.now().strftime('%Y-%m-%d')

    # Generate reports
    markdown_report = generate_markdown_report(validation_results, change_results, processing_time)

    # Save Markdown report
    markdown_file = audit_folder / f'validation_and_changes_report_{timestamp}.md'
    with open(markdown_file, 'w', encoding='utf-8') as f:
        f.write(markdown_report)

    logger.debug(f'Saved Markdown report: {markdown_file.name}')

    # Save Excel report with multiple sheets
    excel_file = audit_folder / f'validation_and_changes_report_{timestamp}.xlsx'
    save_excel_report(excel_file, validation_results, change_results, logger)

    return {'markdown_file': str(markdown_file), 'excel_file': str(excel_file)}


def generate_markdown_report(validation_results: dict, change_results: dict, processing_time: float) -> str:
    """Generate comprehensive Markdown report with per-file summaries"""

    report = []
    report.append('# Data Processing Report')
    report.append(f'\n**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    report.append(f'**Processing Time:** {processing_time:.2f} seconds')
    report.append('\n---\n')

    # === Validation Section ===
    report.append('## Data Quality Validation\n')

    if validation_results.get('has_issues'):
        report.append('⚠️ **Status:** Issues Found\n')
    else:
        report.append('✓ **Status:** All Checks Passed\n')

    # Contract-Vendor relationship
    contracts_with_issues = validation_results.get('contracts_with_multiple_vendors', [])
    if contracts_with_issues:
        report.append('### ⚠️ Contract-Vendor Relationship Issues\n')
        report.append(f'**Count:** {len(contracts_with_issues)} contracts with multiple vendors\n')
    else:
        report.append('### ✓ Contract-Vendor Relationship\n')
        report.append('All contracts have consistent vendor codes.\n')

    # Blank Vendor Catalogue
    blank_count = validation_results.get('blank_vendor_catalogue_count', 0)
    permitted_count = validation_results.get('permitted_blank_count', 0)

    report.append('\n### Blank Vendor Catalogue\n')
    report.append(f'**Total Blank:** {blank_count}\n')
    report.append(f'**Permitted Blank:** {permitted_count}\n')

    if blank_count > permitted_count:
        report.append(f'⚠️ **Unexpected Blank:** {blank_count - permitted_count}\n')
    else:
        report.append('✓ All blank values are permitted\n')

    # Vendor Catalogue Consistency
    inconsistent_count = validation_results.get('inconsistent_vendor_catalogue_count', 0)

    report.append('\n### Vendor Catalogue Consistency\n')
    if inconsistent_count > 0:
        report.append(f'⚠️ **Inconsistent Combinations:** {inconsistent_count}\n')
        report.append('Same PMM Item + Vendor Code + Corp Acct have different catalogues\n')
    else:
        report.append('✓ All combinations have consistent catalogues\n')

    # === Change Tracking Section ===
    report.append('\n---\n')
    report.append('## Change Tracking Summary\n')

    if change_results.get('has_changes'):
        changes_summary = change_results.get('changes_summary', {})

        # Overall summary
        report.append('### Overall Changes\n')
        report.append(f'- **New Rows:** {changes_summary.get("new_rows", 0):,}\n')
        report.append(f'- **Updated Rows:** {changes_summary.get("updated_rows", 0):,}\n')
        report.append(f'- **Skipped Rows (Outdated):** {changes_summary.get("skipped_rows", 0):,}\n')
        report.append(f'- **Files Processed:** {changes_summary.get("files_processed", 0)}\n')

        # Per-file breakdown
        per_file_summary = changes_summary.get('per_file_summary', [])
        if per_file_summary:
            report.append('\n### Per-File Breakdown\n')
            for file_info in per_file_summary:
                report.append(f'\n**File {file_info["file_index"]}: {file_info["file"]}**\n')
                report.append(f'- Original Rows: {file_info.get("original_rows", 0):,}\n')
                report.append(f'- Rows Dropped (Outdated): {file_info.get("dropped_rows", 0):,}\n')
                report.append(f'- New Rows: {file_info["new_rows"]:,}\n')
                report.append(f'- Updated Rows: {file_info["updated_rows"]:,}\n')

                # Show breakdown by date if available
                date_breakdown = file_info.get('date_breakdown')
                if date_breakdown is not None and len(date_breakdown) > 0:
                    report.append('\n**Breakdown by Update Date:**\n')
                    for row in date_breakdown.iter_rows(named=True):
                        update_date = row[Columns0031.ITEM_UPDATE_DATE]
                        row_count = row['row_count']
                        report.append(f'- {update_date}: {row_count:,} rows\n')
                else:
                    report.append(f'- Latest Update Date: {file_info["latest_update_date"]}\n')

        # Duplicate items summary
        duplicates_summary = change_results.get('duplicates_summary')
        if duplicates_summary and duplicates_summary.get('duplicate_count', 0) > 0:
            report.append('\n### Items Updated Across Multiple Files\n')
            report.append(f'- **Items Updated Multiple Times:** {duplicates_summary["duplicate_count"]:,}\n')
            report.append(f'- **Total Rows Before Deduplication:** {duplicates_summary["total_rows_before_dedup"]:,}\n')
            report.append('\n*See "Duplicate Items" sheet in Excel report for details*\n')

    else:
        report.append('No changes detected.\n')

    report.append('\n---\n')
    report.append('*End of Report*\n')

    return ''.join(report)


def save_excel_report(excel_file: Path, validation_results: dict, change_results: dict, logger: logging.Logger | None = None):
    """
    Save comprehensive Excel report with multiple sheets using xlsxwriter

    Args:
        excel_file: Path to save Excel file
        validation_results: Validation results dictionary
        change_results: Change tracking results dictionary
        logger: Logger instance
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    # Import xlsxwriter for multi-sheet Excel files
    import xlsxwriter

    logger.debug(f'Creating Excel report: {excel_file.name}')
    workbook = xlsxwriter.Workbook(excel_file)

    try:
        # === Sheet 1: Summary ===
        summary_data = []

        # Validation summary
        summary_data.append(
            {
                'Category': 'Validation',
                'Metric': 'Status',
                'Value': 'Issues Found' if validation_results.get('has_issues') else 'All Checks Passed',
            }
        )
        summary_data.append(
            {
                'Category': 'Validation',
                'Metric': 'Contracts with Multiple Vendors',
                'Value': len(validation_results.get('contracts_with_multiple_vendors', [])),
            }
        )
        summary_data.append(
            {
                'Category': 'Validation',
                'Metric': 'Blank Vendor Catalogues',
                'Value': validation_results.get('blank_vendor_catalogue_count', 0),
            }
        )
        summary_data.append(
            {
                'Category': 'Validation',
                'Metric': 'Inconsistent Vendor Catalogues',
                'Value': validation_results.get('inconsistent_vendor_catalogue_count', 0),
            }
        )

        # Change tracking summary
        if change_results.get('has_changes'):
            changes_summary = change_results.get('changes_summary', {})
            summary_data.append({'Category': 'Changes', 'Metric': 'New Rows', 'Value': changes_summary.get('new_rows', 0)})
            summary_data.append(
                {'Category': 'Changes', 'Metric': 'Updated Rows', 'Value': changes_summary.get('updated_rows', 0)}
            )
            summary_data.append(
                {'Category': 'Changes', 'Metric': 'Skipped Rows (Outdated)', 'Value': changes_summary.get('skipped_rows', 0)}
            )
            summary_data.append(
                {'Category': 'Changes', 'Metric': 'Files Processed', 'Value': changes_summary.get('files_processed', 0)}
            )

            # Duplicate items
            duplicates_summary = change_results.get('duplicates_summary')
            if duplicates_summary:
                summary_data.append(
                    {
                        'Category': 'Duplicates',
                        'Metric': 'Items Updated Multiple Times',
                        'Value': duplicates_summary.get('duplicate_count', 0),
                    }
                )

        summary_df = pl.DataFrame(summary_data)
        _write_dataframe_to_worksheet(workbook, summary_df, 'Summary', logger)

        # === Sheet 2: Per-File Summary ===
        if change_results.get('has_changes'):
            per_file_summary = change_results.get('changes_summary', {}).get('per_file_summary', [])
            if per_file_summary:
                # Create DataFrame and explicitly specify column order
                per_file_df = pl.DataFrame(per_file_summary)

                # Drop the date_breakdown column (it's a DataFrame, can't write to Excel cell)
                if 'date_breakdown' in per_file_df.columns:
                    per_file_df = per_file_df.drop('date_breakdown')

                # Now rename
                per_file_df = per_file_df.rename(
                    {
                        'file': 'File Name',
                        'file_index': 'File #',
                        'original_rows': 'Original Rows',
                        'dropped_rows': 'Rows Dropped',
                        'new_rows': 'New Rows',
                        'updated_rows': 'Updated Rows',
                        'latest_update_date': 'Latest Update Date',
                    }
                )
                
                # Reorder columns
                per_file_df = per_file_df.select(
                    [
                        'File Name',
                        'File #',
                        'Original Rows',
                        'Rows Dropped',
                        'New Rows',
                        'Updated Rows',
                        'Latest Update Date',
                    ]
                )
                _write_dataframe_to_worksheet(workbook, per_file_df, 'Per-File Summary', logger)

        # === Sheet 3: Date Breakdown ===
        if change_results.get('has_changes'):
            per_file_summary = change_results.get('changes_summary', {}).get('per_file_summary', [])
            if per_file_summary:
                all_date_breakdowns = []
                for file_info in per_file_summary:
                    date_breakdown = file_info.get('date_breakdown')
                    if date_breakdown is not None and len(date_breakdown) > 0:
                        # Add file identifier column
                        breakdown_with_file = date_breakdown.with_columns(pl.lit(file_info['file']).alias('File Name'))
                        all_date_breakdowns.append(breakdown_with_file)

                if all_date_breakdowns:
                    combined_breakdown = pl.concat(all_date_breakdowns, how='diagonal')
                    combined_breakdown = combined_breakdown.select(['File Name', 'Item Update Date', 'row_count'])
                    combined_breakdown = combined_breakdown.rename({'row_count': 'Row Count'})
                    _write_dataframe_to_worksheet(workbook, combined_breakdown, 'Accepted Rows by Date', logger)

        # === Sheet 4: New Rows (UPDATED - Full records) ===
        new_rows_df = change_results.get('new_rows_df')
        if new_rows_df is not None and len(new_rows_df) > 0:
            # Now we just write the dataframe directly as it's already in the correct format
            _write_dataframe_to_worksheet(workbook, new_rows_df, 'New Rows', logger)

        # === Sheet 5: Updated Rows (KEEP AS-IS - Field-level changes) ===
        updated_rows_df = change_results.get('updated_rows_df')
        if updated_rows_df is not None and len(updated_rows_df) > 0:
            _write_dataframe_to_worksheet(workbook, updated_rows_df, 'Updated Rows', logger)

        # === Sheet 6: Duplicate Items - Summary ===
        duplicates_analysis_df = change_results.get('duplicates_analysis_df')
        if duplicates_analysis_df is not None and len(duplicates_analysis_df) > 0:
            # Convert list columns to readable strings
            dup_summary_df = (
                duplicates_analysis_df.with_columns(
                    [
                        pl.col('Update_Dates').list.eval(pl.element().cast(pl.Utf8)).list.join(', ').alias('Update_Dates_str'),
                        pl.col('Prices').list.eval(pl.element().cast(pl.Utf8)).list.join(', ').alias('Prices_str'),
                    ]
                )
                .drop(['Update_Dates', 'Prices'])
                .rename({'occurrence_count': 'Times Updated', 'Update_Dates_str': 'All Update Dates', 'Prices_str': 'All Prices'})
            )

            # Add brackets back to the list columns
            dup_summary_df = dup_summary_df.with_columns(
                [
                    pl.concat_str([pl.lit('['), pl.col('All Update Dates'), pl.lit(']')]).alias('All Update Dates'),
                    pl.concat_str([pl.lit('['), pl.col('All Prices'), pl.lit(']')]).alias('All Prices'),
                ]
            )

            _write_dataframe_to_worksheet(workbook, dup_summary_df, 'Duplicate Items - Summary', logger)

        # === Sheet 7: Duplicate Items - All Versions ===
        duplicates_full_df = change_results.get('duplicates_full_df')
        if duplicates_full_df is not None and len(duplicates_full_df) > 0:
            _write_dataframe_to_worksheet(workbook, duplicates_full_df, 'Duplicate Items - All Versions', logger)

        # === Sheet 8: Validation Issues ===
        validation_issues = []

        # Contract-Vendor issues - FIXED
        contracts_with_issues = validation_results.get('contracts_with_multiple_vendors', [])
        if contracts_with_issues:
            for item in contracts_with_issues:
                contract_no = item.get('Contract No')
                vendor_codes = item.get('vendor_codes', [])
                validation_issues.append(
                    {
                        'Issue Type': 'Contract-Vendor Mismatch',
                        'Contract No': contract_no,
                        'Details': f'Multiple vendors: {", ".join(str(v) for v in vendor_codes)}',
                    }
                )

        # Blank vendor catalogue issues - FIXED
        blank_catalogue_df = validation_results.get('blank_vendor_catalogue_df')
        if blank_catalogue_df is not None and len(blank_catalogue_df) > 0:
            # Get PMM Item Numbers from the dataframe
            pmm_items = blank_catalogue_df.get_column(Columns0031.PMM_ITEM_NUMBER).to_list()
            for pmm_item in pmm_items[:100]:  # Limit to first 100
                validation_issues.append(
                    {
                        'Issue Type': 'Unexpected Blank Vendor Catalogue',
                        Columns0031.PMM_ITEM_NUMBER: pmm_item,
                        'Details': 'Vendor Catalogue is blank but not in permitted list',
                    }
                )

        # Inconsistent vendor catalogue - NO CHANGE (already correct)
        inconsistent_items = validation_results.get('inconsistent_vendor_catalogue_items', [])
        if inconsistent_items:
            for item in inconsistent_items:
                validation_issues.append(
                    {
                        'Issue Type': 'Inconsistent Vendor Catalogue',
                        Columns0031.PMM_ITEM_NUMBER: item.get('pmm_item'),
                        Columns0031.VENDOR_CODE: item.get('vendor_code'),
                        'Vendor Seq': item.get('vendor_seq'),
                        Columns0031.CORP_ACCT: item.get('corp_acct'),
                        'Details': f'Found {item.get("unique_catalogues", 0)} different catalogues',
                        'Vendor Catalogue': item.get('catalogue_values'),
                    }
                )

        if validation_issues:
            issues_df = pl.DataFrame(validation_issues)
            _write_dataframe_to_worksheet(workbook, issues_df, 'Validation Issues', logger)

        logger.debug(f'Excel report saved with {len(workbook.sheetnames)} sheets')

    finally:
        workbook.close()

    logger.debug(f'Excel report saved: {excel_file.name}')


def _write_dataframe_to_worksheet(workbook, df: pl.DataFrame, sheet_name: str, logger: logging.Logger | None = None):
    """
    Helper function to write a Polars DataFrame to an Excel worksheet

    Args:
        workbook: xlsxwriter Workbook object
        df: Polars DataFrame to write
        sheet_name: Name of the worksheet
        logger: Logger instance
    """
    if logger is None:
        logger = logging.getLogger('data_pipeline.sync')

    worksheet = workbook.add_worksheet(sheet_name[:31])  # Excel sheet name limit is 31 chars

    # Write headers
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3'})

    # Create date format
    date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})

    for col_num, column_name in enumerate(df.columns):
        worksheet.write(0, col_num, column_name, header_format)

    # Write data
    for row_num, row in enumerate(df.iter_rows(), start=1):
        for col_num, value in enumerate(row):
            # Convert lists to strings for Excel
            if isinstance(value, list):
                value = str(value)

            # Apply date format for date columns
            column_name = df.columns[col_num]
            # if 'date' in column_name.lower() and value is not None:
            if re.search(r'\bdate\b', column_name.lower()) and value is not None:
                worksheet.write(row_num, col_num, value, date_format)
            else:
                worksheet.write(row_num, col_num, value)

    # Auto-adjust column widths (approximate)
    for col_num, column_name in enumerate(df.columns):
        max_length = len(str(column_name))
        for row in df.iter_rows():
            cell_value = str(row[col_num]) if row[col_num] is not None else ''
            max_length = max(max_length, len(cell_value))
        worksheet.set_column(col_num, col_num, min(max_length + 2, 50))  # Cap at 50 chars

    logger.debug(f'  Sheet "{sheet_name[:31]}": {len(df)} rows, {len(df.columns)} columns')
