this is readme

## Expected Row Count Discrepancies

The database may show fewer net new rows than the incremental file contains.
This is expected when:
- Historical duplicate records exist (same 5-key columns, different VPN)
- Incremental update provides the corrected version
- Process removes ALL duplicates and replaces with single correct record

Example:
- Incremental file: +100 rows
- Database duplicates cleaned: -15 rows  
- Net database change: +85 rows

This is a feature, not a bug - gradually cleaning data quality issues.
