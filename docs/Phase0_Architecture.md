# Phase 0 (Sync) Architecture Documentation

**Project:** CLS Allscripts Data Processing Pipeline  
**Generated:** 2025-11-08  
**Python Version:** 3.12

---

## Table of Contents

1. [File Structure](#file-structure)
2. [Function Call Hierarchy](#function-call-hierarchy)
3. [Module Functions Summary](#module-functions-summary)
   - [main.py](#mainpy)
   - [logging_config.py](#logging_configpy)
   - [database_sync.py](#database_syncpy)
   - [sync_core/orchestrator.py](#sync_coreorchestratorpy)
   - [sync_core/processing.py](#sync_coreprocessingpy)
   - [sync_core/quality.py](#sync_corequalitypy)
   - [sync_core/reporting.py](#sync_corereportingpy)
   - [sync_core/files.py](#sync_corefilespy)
   - [sync_core/backup.py](#sync_corebackuppy)
   - [sync_core/state.py](#sync_corestatepy)
4. [Logger Hierarchy](#logger-hierarchy)
5. [Design Patterns](#design-patterns)
6. [Configuration](#configuration)

---

## File Structure

```
cls_project/
├── main.py                          # Main entry point
├── config/
│   └── config.json                  # Configuration
├── logs/                            # Log files (daily)
│   └── pipeline_YYYYMMDD.log
├── src/
│   ├── logging_config.py           # Logging setup
│   ├── database_sync.py            # Phase 0 public API
│   ├── integrate.py                # Phase 1 (not covered here)
│   ├── classification.py           # Phase 2 (not covered here)
│   ├── export.py                   # Phase 3 (not covered here)
│   └── sync_core/                  # Phase 0 internal implementation
│       ├── __init__.py
│       ├── orchestrator.py         # Main coordination logic
│       ├── processing.py           # Data processing operations
│       ├── quality.py              # Validation and change tracking
│       ├── reporting.py            # Report generation
│       ├── files.py                # File operations
│       ├── backup.py               # Backup utilities
│       └── state.py                # State management
└── data/
    ├── database/
    │   ├── 0031.parquet           # Main database
    │   ├── backup/                # Parquet backups
    │   ├── audit/                 # Validation reports
    │   └── parquet_state.json     # State tracking
    ├── reports/
    │   ├── 0031/                  # Incremental files
    │   └── archive/               # Archived incrementals
    ├── daily_files/               # Phase 1 input
    ├── integrated/                # Phase 1 output
    ├── classified/                # Phase 2 output
    └── exports/                   # Phase 3 output
```

[↑ Back to TOC](#table-of-contents)

---

## Function Call Hierarchy

<details>
<summary><strong>Click to expand full call hierarchy</strong></summary>

```
main.py
├── main()
│   ├── load_config()
│   ├── get_config_paths()
│   ├── setup_logging()                              [from logging_config.py]
│   │   └── Returns: (logger, log_file)
│   │
│   └── [Command: sync]
│       └── process_sync()                           [from database_sync.py]
│           └── auto_check_and_update()              [from database_sync.py]
│               └── update_parquet_if_needed()       [from sync_core/orchestrator.py]
│                   │
│                   ├── load_state()                 [from sync_core/state.py]
│                   │
│                   ├── get_weekly_full_files()      [from sync_core/files.py]
│                   │
│                   ├── get_incremental_files()      [from sync_core/files.py]
│                   │
│                   ├── [MODE 1: Weekly Full Backup Found]
│                   │   └── process_weekly_full_backup()  [from sync_core/processing.py]
│                   │       ├── create_backup()           [from sync_core/backup.py]
│                   │       ├── clean_dataframe()         [internal - processing.py]
│                   │       ├── convert_and_optimize_columns()  [internal - processing.py]
│                   │       ├── validate_parquet_data()   [from sync_core/quality.py]
│                   │       │   ├── Check Contract-Vendor relationship
│                   │       │   ├── Check blank Vendor Catalogue
│                   │       │   └── Check Vendor Catalogue consistency
│                   │       └── cleanup_old_full_backups() [from sync_core/files.py]
│                   │
│                   ├── [MODE 2: Incremental Files Found]
│                   │   └── apply_incremental_update()    [from sync_core/processing.py]
│                   │       ├── create_backup()           [from sync_core/backup.py]
│                   │       │   └── Creates timestamped .parquet backup
│                   │       │
│                   │       ├── Load incremental Excel file(s)
│                   │       │   └── process_excel_files()     [internal - processing.py]
│                   │       │       ├── Read Excel files
│                   │       │       ├── Normalize string columns (strip)
│                   │       │       └── Concatenate DataFrames
│                   │       │
│                   │       ├── clean_dataframe()         [internal - processing.py]
│                   │       │   ├── Trim all string columns
│                   │       │   ├── Convert blank strings to None
│                   │       │   └── Trim column names
│                   │       │
│                   │       ├── convert_and_optimize_columns()  [internal - processing.py]
│                   │       │   ├── Convert date columns (multiple formats)
│                   │       │   ├── Optimize integer columns
│                   │       │   └── Optimize float columns
│                   │       │
│                   │       ├── [BATCH MODE: Multiple files]
│                   │       │   ├── Track changes per file
│                   │       │   │   └── track_row_changes()   [from sync_core/quality.py]
│                   │       │   │       ├── Create unique keys (5-column)
│                   │       │   │       ├── Identify new vs updated rows
│                   │       │   │       ├── Compare field-level changes
│                   │       │   │       └── Generate date breakdown
│                   │       │   │
│                   │       │   ├── Analyze duplicate items across files
│                   │       │   │   ├── Group by unique key
│                   │       │   │   ├── Find items updated multiple times
│                   │       │   │   └── Create duplicate analysis DataFrames
│                   │       │   │
│                   │       │   └── Aggregate all change results
│                   │       │       ├── Combine changes from all files
│                   │       │       ├── Create per-file summary
│                   │       │       └── Calculate totals
│                   │       │
│                   │       ├── [SINGLE MODE: One file]
│                   │       │   └── track_row_changes()       [from sync_core/quality.py]
│                   │       │       └── Simple change tracking
│                   │       │
│                   │       ├── Deduplicate merged data
│                   │       │   ├── Sort by Item Update Date
│                   │       │   └── Keep last occurrence (unique by 5-col key)
│                   │       │
│                   │       ├── Merge with database
│                   │       │   ├── Create merge keys
│                   │       │   ├── Identify updates vs new records
│                   │       │   ├── Remove old versions
│                   │       │   └── Add all incremental data
│                   │       │
│                   │       ├── Write updated parquet
│                   │       │
│                   │       ├── validate_parquet_data()   [from sync_core/quality.py]
│                   │       │   ├── Load permitted blank VPN list
│                   │       │   ├── Check Contract-Vendor relationship
│                   │       │   ├── Check blank Vendor Catalogue
│                   │       │   └── Check Vendor Catalogue consistency
│                   │       │
│                   │       └── archive_file()            [from sync_core/files.py]
│                   │           ├── Move file to archive folder
│                   │           └── Add timestamp if collision
│                   │
│                   ├── [MODE 3: Fallback - Full Rebuild]
│                   │   ├── check_for_changes()          [from sync_core/files.py]
│                   │   │   ├── get_excel_files()         [from sync_core/files.py]
│                   │   │   ├── load_state()              [from sync_core/state.py]
│                   │   │   ├── Compare file timestamps
│                   │   │   └── Detect new/modified/deleted files
│                   │   │
│                   │   ├── create_backup()               [from sync_core/backup.py]
│                   │   │
│                   │   ├── rebuild_parquet()             [from sync_core/processing.py]
│                   │   │   ├── get_excel_files()         [from sync_core/files.py]
│                   │   │   ├── process_excel_files()     [internal - processing.py]
│                   │   │   ├── clean_dataframe()         [internal - processing.py]
│                   │   │   ├── convert_and_optimize_columns()  [internal - processing.py]
│                   │   │   └── validate_parquet_data()   [from sync_core/quality.py]
│                   │   │
│                   │   └── track_row_changes()           [from sync_core/quality.py]
│                   │
│                   ├── save_combined_report()            [from sync_core/reporting.py]
│                   │   ├── generate_markdown_report()    [internal - reporting.py]
│                   │   │   ├── Validation summary
│                   │   │   ├── Change summary
│                   │   │   ├── Per-file breakdown
│                   │   │   └── Duplicate items summary
│                   │   │
│                   │   └── save_excel_report()           [internal - reporting.py]
│                   │       └── _write_dataframe_to_worksheet()  [internal - reporting.py]
│                   │           ├── Sheet 1: Summary
│                   │           ├── Sheet 2: Per-File Summary
│                   │           ├── Sheet 3: Date Breakdown
│                   │           ├── Sheet 4: All Changes
│                   │           ├── Sheet 5: New Rows
│                   │           ├── Sheet 6: Updated Rows
│                   │           ├── Sheet 7: Duplicate Items - Summary
│                   │           ├── Sheet 8: Duplicate Items - All Versions
│                   │           └── Sheet 9: Validation Issues
│                   │
│                   ├── save_state()                      [from sync_core/state.py]
│                   │   └── Save to parquet_state.json
│                   │
│                   ├── cleanup_old_backups()             [from sync_core/backup.py]
│                   │   ├── Find backups older than retention period
│                   │   └── Delete old backups
│                   │
│                   └── cleanup_old_archives()            [from sync_core/files.py]
│                       ├── Find archives older than retention period
│                       └── Delete old archives
```

</details>

[↑ Back to TOC](#table-of-contents)

---

## Module Functions Summary

### main.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Main entry point and CLI interface for the pipeline

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `main()` | CLI entry point | None | None |
| `load_config()` | Load configuration from JSON | None | `dict` |
| `get_config_paths()` | Build paths dictionary | `config: dict` | `dict` |
| `run_all_phases()` | Run all phases sequentially | `config: dict, paths: dict, logger: Logger` | `bool` |
| `show_status()` | Display pipeline status | `config: dict, paths: dict, logger: Logger` | None |
| `print_usage()` | Show help information | None | None |

**CLI Commands:**
- `python main.py all` - Run all phases
- `python main.py sync` - Phase 0: Database sync
- `python main.py integrate` - Phase 1: Integration
- `python main.py classify` - Phase 2: Classification
- `python main.py export` - Phase 3: Export
- `python main.py status` - Show pipeline status

**Logger:** `data_pipeline` (root logger)

</details>

[↑ Back to TOC](#table-of-contents)

---

### logging_config.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Setup dual logging system (console + file)

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `setup_logging()` | Initialize logging system | `log_folder: Path, console_level: str, file_level: str` | `tuple[Logger, Path]` |

**Features:**
- **Daily log files:** One file per day (e.g., `pipeline_20251108.log`)
- **Console handler:** Brief output (INFO level by default)
- **File handler:** Detailed output with module names (DEBUG level)
- **Format:** `%(asctime)s - %(name)s - %(levelname)-8s - %(message)s`

**Configuration:**
```json
"logging": {
  "console_level": "INFO",
  "file_level": "DEBUG",
  "log_folder": "logs",
  "retention_days": 30
}
```

</details>

[↑ Back to TOC](#table-of-contents)

---

### database_sync.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Public API for Phase 0 (Database Sync)

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `process_sync()` | Phase 0 entry point | `config: dict, paths: dict, logger: Logger \| None` | `bool` |
| `auto_check_and_update()` | Main sync function | `config: dict, paths: dict, logger: Logger \| None` | `bool` |
| `daily_update()` | Daily update check | `config: dict, paths: dict, logger: Logger \| None` | `bool` |
| `force_update()` | Force full rebuild | `config: dict, paths: dict, logger: Logger \| None` | `bool` |
| `apply_incremental()` | Apply single incremental | `config: dict, paths: dict, file_path: str, logger: Logger \| None` | `bool` |
| `get_status()` | Get database status | `config: dict, paths: dict` | `dict` |
| `print_status()` | Print status | `config: dict, paths: dict, logger: Logger \| None` | None |

**Import Path:** `from .sync_core.orchestrator import update_parquet_if_needed`

**Logger:** `data_pipeline.sync`

</details>

[↑ Back to TOC](#table-of-contents)

---

### sync_core/orchestrator.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Main coordination logic for parquet update operations

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `update_parquet_if_needed()` | Main orchestration logic | `config: dict, paths: dict, force_rebuild: bool, incremental_file: Path \| None, logger: Logger \| None` | `bool` |
| `_clean_change_summary_for_state()` | Clean summary for JSON | `change_summary: dict \| None` | `dict \| None` |

**Processing Modes:**
1. **Legacy Mode:** Single incremental file provided
2. **Weekly Full Mode:** New weekly full backup detected
3. **Incremental Mode:** Daily incremental files found
4. **Fallback Mode:** Full rebuild from all Excel files

**Key Logic:**
- Auto-detects file type (weekly full vs incremental)
- Tracks applied incrementals in state file
- Handles batch processing of multiple incrementals
- Creates backups before any modifications
- Generates combined validation + change reports
- Cleans up old backups and archives

**Logger:** `data_pipeline.sync.orchestrator`

</details>

[↑ Back to TOC](#table-of-contents)

---

### sync_core/processing.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Data processing operations - cleaning, updates, and rebuilds

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `apply_incremental_update()` | Apply incremental updates | `db_file: Path, incremental_files: Path \| list[Path], config: dict, backup_folder: Path, audit_folder: Path, archive_folder: Path \| None, blank_vpn_permitted_file: Path \| None, logger: Logger \| None` | `tuple[DataFrame, dict, dict]` |
| `process_weekly_full_backup()` | Process weekly full backup | `weekly_file: Path, config: dict, paths: dict, logger: Logger \| None` | `tuple[DataFrame, dict]` |
| `rebuild_parquet()` | Full rebuild from Excel | `main_folder: Path, db_file: Path, config: dict, skip_cleaning: bool, blank_vpn_permitted_file: Path \| None, logger: Logger \| None` | `tuple[DataFrame, dict]` |
| `process_excel_files()` | Load and concat Excel files | `file_paths: list[Path], infer_schema_length: int, logger: Logger \| None` | `DataFrame \| None` |
| `clean_dataframe()` | Clean and normalize data | `df: DataFrame, logger: Logger \| None` | `DataFrame` |
| `convert_and_optimize_columns()` | Convert dates and optimize | `df: DataFrame, config: dict, logger: Logger \| None` | `DataFrame` |
| `apply_categorical_types()` | Apply categorical types | `df: DataFrame, config: dict` | `DataFrame` |
| `convert_date_columns()` | DEPRECATED: Convert dates | `df: DataFrame, date_columns: list[str], logger: Logger \| None` | `DataFrame` |

**Key Features:**
- **Batch processing:** Handles single or multiple incremental files efficiently
- **Per-file tracking:** Tracks changes from each file separately in batch mode
- **Duplicate detection:** Finds items updated across multiple files
- **5-column unique key:** `PMM Item Number + Corp Acct + Vendor Code + Additional Cost Centre + Additional GL Account`
- **Deduplication:** Keeps last occurrence based on `Item Update Date`
- **Data optimization:** Date conversion, integer/float optimization, categorical types

**Logger:** `data_pipeline.sync.processing`

</details>

[↑ Back to TOC](#table-of-contents)

---

### sync_core/quality.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Data quality validation and change tracking

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `validate_parquet_data()` | Validate data quality | `df: DataFrame, blank_vpn_permitted_file: Path \| None, logger: Logger \| None` | `dict` |
| `track_row_changes()` | Track changes between versions | `current_df: DataFrame, previous_df: DataFrame, audit_folder: Path, logger: Logger \| None` | `dict` |
| `print_change_summary()` | Print change summary | `change_results: dict, logger: Logger \| None` | None |

**Validation Checks:**
1. **Contract-Vendor Relationship:** Contract No should have 1-to-1 with Vendor Code
2. **Blank Vendor Catalogue:** Check for unexpected blank values (with permitted list)
3. **Vendor Catalogue Consistency:** Same PMM+Vendor+CorpAcct should have same catalogue

**Change Tracking:**
- Uses 5-column unique key
- Identifies new vs updated rows
- Tracks field-level changes
- Creates date breakdown
- Generates audit DataFrames

**Returns Dictionary:**
```python
{
    'has_issues': bool,
    'contracts_with_multiple_vendors': list,
    'blank_vendor_catalogue_count': int,
    'inconsistent_vendor_catalogue_count': int,
    'has_changes': bool,
    'changes_summary': dict,
    'changes_df': DataFrame,
    'new_rows_df': DataFrame,
    'updated_rows_df': DataFrame,
    'date_breakdown': DataFrame
}
```

**Logger:** `data_pipeline.sync.quality`

</details>

[↑ Back to TOC](#table-of-contents)

---

### sync_core/reporting.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Generate validation and change tracking reports

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `save_combined_report()` | Save validation + change reports | `validation_results: dict, change_results: dict, processing_time: float, audit_folder: Path, logger: Logger \| None` | `dict` |
| `generate_markdown_report()` | Generate markdown report | `validation_results: dict, change_results: dict, processing_time: float` | `str` |
| `save_excel_report()` | Save Excel report | `excel_file: Path, validation_results: dict, change_results: dict, logger: Logger \| None` | None |
| `_write_dataframe_to_worksheet()` | Write DataFrame to Excel | `workbook, df: DataFrame, sheet_name: str, logger: Logger \| None` | None |

**Report Files:**
1. **Markdown:** `validation_and_changes_report_YYYY-MM-DD.md`
2. **Excel:** `validation_and_changes_report_YYYY-MM-DD.xlsx`

**Excel Sheets:**
1. Summary
2. Per-File Summary
3. Date Breakdown
4. All Changes
5. New Rows
6. Updated Rows
7. Duplicate Items - Summary
8. Duplicate Items - All Versions
9. Validation Issues

**Logger:** `data_pipeline.sync.reporting`

</details>

[↑ Back to TOC](#table-of-contents)

---

### sync_core/files.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** File operations and management

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `get_excel_files()` | Get all Excel files | `main_folder: Path, logger: Logger \| None` | `list[Path]` |
| `get_incremental_files()` | Get incremental files | `main_folder: Path, config: dict, logger: Logger \| None` | `list[Path]` |
| `get_weekly_full_files()` | Get weekly full files | `main_folder: Path, config: dict, logger: Logger \| None` | `list[Path]` |
| `parse_date_from_filename()` | Parse date from filename | `filename: str, date_format: str` | `datetime` |
| `get_file_date()` | Get file date | `file_path: Path, config: dict, file_type: str` | `datetime` |
| `check_for_changes()` | Check for file changes | `main_folder: Path, state_file: Path, logger: Logger \| None` | `tuple[bool, dict]` |
| `archive_file()` | Move file to archive | `file_path: Path, archive_folder: Path, logger: Logger \| None` | `Path` |
| `cleanup_old_archives()` | Remove old archives | `archive_folder: Path, retention_days: int, logger: Logger \| None` | None |
| `cleanup_old_full_backups()` | Remove old full backups | `reports_folder: Path, current_full_file: Path, config: dict, logger: Logger \| None` | None |

**File Patterns:**
- **Daily Incremental:** `0031-Contract Item Price Cat Pkg Extract *.xlsx`
- **Weekly Full:** `0031-Contract Item Price Cat Pkg Extract [0-9][0-9][0-9][0-9].xlsx`

**Logger:** `data_pipeline.sync.files`

</details>

[↑ Back to TOC](#table-of-contents)

---

### sync_core/backup.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Backup creation and cleanup

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `create_backup()` | Create timestamped backup | `db_file: Path, backup_folder: Path, logger: Logger \| None` | `Path \| None` |
| `cleanup_old_backups()` | Remove old backups | `backup_folder: Path, retention_days: int, logger: Logger \| None` | None |

**Backup Naming:** `0031_backup_YYYYMMDD_HHMMSS.parquet`

**Default Retention:** 14 days (configured in `config.json`)

**Logger:** `data_pipeline.sync.backup`

</details>

[↑ Back to TOC](#table-of-contents)

---

### sync_core/state.py

<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** State management and status tracking

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `load_state()` | Load state from JSON | `state_file: Path` | `dict` |
| `save_state()` | Save state to JSON | `state_file: Path, state: dict` | None |
| `get_update_status()` | Get current status | `config: dict, paths: dict` | `dict` |
| `print_status()` | Print status | `config: dict, paths: dict, logger: Logger \| None` | None |

**State File:** `data/database/parquet_state.json`

**State Structure:**
```json
{
  "last_update": "2025-11-08T20:30:06",
  "last_full_backup": "2025-11-01T10:00:00",
  "last_full_backup_file": "path/to/file.xlsx",
  "applied_incrementals": ["file1.xlsx", "file2.xlsx"],
  "file_timestamps": {},
  "row_count": 1471864,
  "column_count": 69,
  "last_change_summary": {},
  "last_validation_summary": {}
}
```

**Logger:** `data_pipeline.sync.state`

</details>

[↑ Back to TOC](#table-of-contents)

---

## Logger Hierarchy

```
data_pipeline                              # Root logger (main.py)
│                                          # Logger from setup_logging()
│
└── data_pipeline.sync                     # database_sync.py
    │                                      # Public API entry point
    │
    └── data_pipeline.sync.orchestrator    # orchestrator.py
        │                                  # Main coordination logic
        │
        ├── data_pipeline.sync.processing  # processing.py
        │                                  # Data processing operations
        │
        ├── data_pipeline.sync.quality     # quality.py
        │                                  # Validation and change tracking
        │
        ├── data_pipeline.sync.reporting   # reporting.py
        │                                  # Report generation
        │
        ├── data_pipeline.sync.files       # files.py
        │                                  # File operations
        │
        ├── data_pipeline.sync.backup      # backup.py
        │                                  # Backup utilities
        │
        └── data_pipeline.sync.state       # state.py
                                           # State management
```

**Logger Propagation:**
- Each module creates its own logger: `logging.getLogger('data_pipeline.module.submodule')`
- Loggers inherit handlers from parent `data_pipeline` logger
- Messages are formatted with module name: `%(name)s`
- No logger objects passed between submodules (each creates its own)

**Console Output (INFO level):**
```
=== Data Processing Pipeline ===
Command: sync
=== Checking Parquet File Status ===
✓ Found 1 unapplied incremental file(s)
Loading existing parquet: 0031.parquet
✓ Incremental update completed successfully
```

**Log File (DEBUG level):**
```
2025-11-08 20:29:53 - data_pipeline.sync.orchestrator - INFO     - === Checking Parquet File Status ===
2025-11-08 20:29:53 - data_pipeline.sync.files - DEBUG    - Found 1 incremental file(s) matching pattern: ...
2025-11-08 20:29:53 - data_pipeline.sync.processing - INFO     - Loading existing parquet: 0031.parquet
2025-11-08 20:29:53 - data_pipeline.sync.processing - DEBUG    -   Current rows: 1,471,246
2025-11-08 20:29:53 - data_pipeline.sync.backup - DEBUG    - Creating backup: 0031_backup_20251108_202953.parquet
```

[↑ Back to TOC](#table-of-contents)

---

## Design Patterns

<details>
<summary><strong>Click to expand</strong></summary>

### 1. **Wrapper Pattern**
- `database_sync.py` is a thin public API wrapper
- Internal implementation in `sync_core/` modules
- Allows for clean separation of interface and implementation

### 2. **Orchestrator Pattern**
- `orchestrator.py` coordinates all operations
- Delegates specific tasks to specialized modules
- Handles multiple processing modes (weekly full, incremental, fallback)

### 3. **State Management Pattern**
- `state.py` manages persistent state in JSON
- Tracks applied incrementals, timestamps, and metadata
- Enables incremental processing and change detection

### 4. **Modular Logging Pattern**
- Each module creates its own logger with hierarchical naming
- Logger objects NOT passed between modules
- Consistent format with module names for traceability

### 5. **Batch Processing Pattern**
- `apply_incremental_update()` handles both single and multiple files
- Per-file change tracking in batch mode
- Efficient: loads all files, processes once, writes once

### 6. **Backup & Recovery Pattern**
- Always creates backup before modifications
- Automatic restoration on failure
- Configurable retention periods

### 7. **Validation & Reporting Pattern**
- Validation integrated into processing pipeline
- Combined reports (validation + changes) generated together
- Multiple output formats (Markdown + Excel)

### 8. **Configuration-Driven Pattern**
- All paths, patterns, and settings in `config.json`
- File patterns defined with glob expressions
- Processing options (schema inference, type optimization) configurable

</details>

[↑ Back to TOC](#table-of-contents)

---

## Configuration

<details>
<summary><strong>Click to expand key configuration sections</strong></summary>

### File Patterns

```json
"file_patterns": {
  "daily_incremental": {
    "pattern": "0031-Contract Item Price Cat Pkg Extract *.xlsx",
    "date_format": "0031-Contract Item Price Cat Pkg Extract %Y_%m_%d*.xlsx",
    "archive_after_processing": true
  },
  "weekly_full": {
    "pattern": "0031-Contract Item Price Cat Pkg Extract [0-9][0-9][0-9][0-9].xlsx",
    "date_format": "0031-Contract Item Price Cat Pkg Extract %m%d.xlsx",
    "keep_only_latest": true
  }
}
```

### Data Processing

```json
"data_processing": {
  "date_columns": [
    "Contract EFF Date",
    "Contract EXP Date",
    "Item Create Date",
    "Item Update Date"
  ],
  "type_optimization": {
    "integer_columns": ["UOM1 QTY", "UOM2 QTY", "UOM3 QTY"],
    "float_columns": ["Default UOM Price", "Price1", "Price2", "Price3"],
    "categorical_columns": ["UOM1", "UOM2", "UOM3", "Corp Acct"]
  },
  "columns_to_drop": [
    "Corporation Info",
    "Item Status",
    "Commodity Code"
  ]
}
```

### Processing Schedule

```json
"processing_schedule": {
  "process_incrementals_on_startup": true,
  "auto_detect_weekly_full": true,
  "max_incrementals_per_run": 10,
  "process_daily_files": true
}
```

### Archive Settings

```json
"archive_settings": {
  "enabled": true,
  "retention_days": 90
}
```

### Update Settings

```json
"update_settings": {
  "backup_retention_days": 14,
  "state_file": "parquet_state.json"
}
```

### Logging

```json
"logging": {
  "console_level": "INFO",
  "file_level": "DEBUG",
  "log_folder": "logs",
  "retention_days": 30
}
```

</details>

[↑ Back to TOC](#table-of-contents)

---

## Data Flow

<details>
<summary><strong>Click to expand data flow diagram</strong></summary>

### Incremental Update Flow

```
Excel Files (Incremental)
    │
    ├─→ Read & Load
    │       ├─→ Normalize strings (strip)
    │       └─→ Concatenate multiple files
    │
    ├─→ Clean & Transform
    │       ├─→ Trim columns
    │       ├─→ Convert blanks to None
    │       ├─→ Convert dates (3 formats)
    │       ├─→ Optimize integers/floats
    │       └─→ Remove duplicates (keep last)
    │
    ├─→ Track Changes (per file in batch mode)
    │       ├─→ Create 5-column unique keys
    │       ├─→ Identify new vs updated rows
    │       ├─→ Track field-level changes
    │       ├─→ Generate date breakdown
    │       └─→ Analyze duplicate items across files
    │
    ├─→ Merge with Database
    │       ├─→ Load existing parquet (1.4M+ rows)
    │       ├─→ Remove old versions of updated records
    │       ├─→ Add all new/updated records
    │       └─→ Write updated parquet
    │
    ├─→ Validate Data Quality
    │       ├─→ Check Contract-Vendor relationships
    │       ├─→ Check blank Vendor Catalogues
    │       └─→ Check Vendor Catalogue consistency
    │
    ├─→ Generate Reports
    │       ├─→ Markdown report
    │       └─→ Excel report (9 sheets)
    │
    ├─→ Update State
    │       ├─→ Add processed files to applied_incrementals
    │       ├─→ Update row/column counts
    │       └─→ Save validation/change summaries
    │
    └─→ Cleanup
            ├─→ Archive processed files
            ├─→ Remove old backups (>14 days)
            └─→ Remove old archives (>90 days)
```

### Weekly Full Backup Flow

```
Excel File (Weekly Full)
    │
    ├─→ Read & Load
    │       └─→ Single large file (~1.4M rows)
    │
    ├─→ Clean & Transform
    │       ├─→ Trim columns
    │       ├─→ Convert blanks to None
    │       ├─→ Convert dates
    │       └─→ Optimize data types
    │
    ├─→ Replace Database
    │       └─→ Overwrite parquet file completely
    │
    ├─→ Validate Data Quality
    │       └─→ Run all validation checks
    │
    ├─→ Generate Reports
    │       └─→ Validation report only (no changes)
    │
    ├─→ Update State
    │       ├─→ Reset applied_incrementals (empty)
    │       ├─→ Set last_full_backup timestamp
    │       └─→ Save full backup file reference
    │
    └─→ Cleanup
            └─→ Remove old weekly full files (keep latest only)
```

</details>

[↑ Back to TOC](#table-of-contents)

---

## Key Concepts

<details>
<summary><strong>Click to expand key concepts</strong></summary>

### Unique Key (5-column)

All change tracking and merging uses a 5-column unique key:
1. `PMM Item Number`
2. `Corp Acct`
3. `Vendor Code`
4. `Additional Cost Centre`
5. `Additional GL Account`

**Why 5 columns?**
- `PMM Item Number` alone is not unique (multiple vendors)
- `PMM + Vendor Code` is not unique (multiple corp accounts)
- `PMM + Vendor + Corp Acct` is not unique (additional cost/GL variations)
- All 5 columns together form a unique combination

### Processing Modes

**1. Weekly Full Mode**
- Triggered when a new weekly full backup file is detected
- Completely replaces the database
- Resets incremental tracking
- Used for baseline refresh

**2. Incremental Mode**
- Processes daily incremental files
- Updates existing records or adds new ones
- Tracks which files have been applied
- Efficient for daily updates

**3. Fallback Mode**
- Used when no specific pattern is detected
- Rebuilds from all Excel files in folder
- Based on file modification timestamps
- Safety mechanism for manual intervention

### Batch Processing

When multiple incremental files are found:
1. **Load all files** into memory (concatenate)
2. **Track changes per file** (maintains source file information)
3. **Analyze duplicates** across files (items updated multiple times)
4. **Deduplicate** before merge (keep last occurrence by date)
5. **Merge once** with database (efficient single write)
6. **Archive all files** at once

Benefits:
- ✅ Efficient (single merge operation)
- ✅ Detailed tracking (per-file summaries)
- ✅ Duplicate detection (cross-file analysis)
- ✅ Atomic operation (all or nothing)

### Change Tracking

**Field-level tracking:**
- Compares each field of updated records
- Records: `Column, Previous Value, Current Value, Update Date, Change Type`
- Separate tracking for new rows vs updated rows

**Per-file tracking (batch mode):**
- Each file's changes tracked independently
- Aggregated summary shows totals across all files
- Date breakdown shows distribution by `Item Update Date`

**Output:**
- Changes DataFrame (all field-level changes)
- New Rows DataFrame (new records only)
- Updated Rows DataFrame (modified records only)
- Per-File Summary (batch mode)
- Duplicate Analysis (items updated multiple times)

### Data Validation

**Three main checks:**

1. **Contract-Vendor Relationship**
   - Ensures one contract = one vendor
   - Flags contracts with multiple vendor codes
   - Critical for contract integrity

2. **Blank Vendor Catalogue**
   - Checks for blank/null Vendor Catalogue values
   - Excludes permitted PMM Item Numbers (from reference file)
   - Identifies unexpected blanks

3. **Vendor Catalogue Consistency**
   - Same PMM + Vendor + Corp Acct should have same catalogue
   - Flags inconsistent combinations
   - Important for data quality

### State Management

**Why track state?**
- Avoid reprocessing same incremental files
- Track which weekly full is current
- Monitor database growth (row/column counts)
- Audit trail (last update time, last validation results)

**State persistence:**
- JSON file: `data/database/parquet_state.json`
- Updated after every successful operation
- Used to resume processing after interruption

</details>

[↑ Back to TOC](#table-of-contents)

---

## Performance Considerations

<details>
<summary><strong>Click to expand performance tips</strong></summary>

### Memory Management

**Large DataFrame operations:**
- Database size: ~1.4M rows × 69 columns
- Incremental files: ~20K rows typical
- Memory usage: ~500MB for full database in memory

**Optimization strategies:**
- Categorical types applied at read-time (not stored in parquet)
- Date/numeric type optimization reduces memory footprint
- Lazy evaluation with Polars (scan_parquet for filters)

### Processing Speed

**Typical timings:**
- Load database: ~1 second
- Read incremental file: ~2 seconds
- Change tracking: ~1-2 seconds
- Merge operation: ~1 second
- Write parquet: ~1 second
- Generate reports: ~10 seconds
- **Total:** ~15-20 seconds per incremental file

**Batch processing advantage:**
- 5 files individually: ~75-100 seconds
- 5 files in batch: ~20-25 seconds
- **Speed improvement:** 3-4x faster

### Disk I/O

**Read operations:**
- Database parquet: ~50MB compressed
- Incremental Excel: ~5-10MB each
- Reference files: <1MB

**Write operations:**
- Updated parquet: ~50MB
- Backup parquet: ~50MB
- Excel reports: ~2-5MB
- Markdown reports: ~10KB

**Recommendations:**
- Use SSD for data folder
- Parquet compression (default: snappy)
- Cleanup old backups/archives regularly

### Scalability

**Current scale:**
- Database: 1.4M rows, 69 columns
- Incremental updates: 20K rows typical
- Weekly full: 1.4M rows

**Scaling limits:**
- Polars can handle 10M+ rows efficiently
- Memory becomes constraint before processing speed
- Consider chunked processing if >5M rows

</details>

[↑ Back to TOC](#table-of-contents)

---

## Error Handling

<details>
<summary><strong>Click to expand error handling strategies</strong></summary>

### Backup & Recovery

**Before every modification:**
1. Create timestamped backup of database
2. If processing fails, backup is preserved
3. Manual restoration possible from backup folder

**Automatic recovery:**
```python
try:
    # Process updates
    process_data()
except Exception as e:
    # Restore from backup
    if backup_path.exists():
        shutil.copy2(backup_path, db_file)
        logger.info('Backup restored')
    raise
```

### Validation Failures

**Non-blocking issues:**
- Data quality warnings logged
- Processing continues
- Issues reported in validation report
- User reviews and addresses issues separately

**Blocking failures:**
- Missing required columns
- File read errors
- Parquet write errors
- Invalid data types

### State Consistency

**State saved only on success:**
- If processing fails, state file not updated
- Next run will retry failed operation
- Applied incrementals list only updated on success

**State recovery:**
- JSON format (human-readable)
- Can be manually edited if corrupted
- Falls back to empty state if file missing

### Logging

**All operations logged:**
- DEBUG: Detailed operations (log file only)
- INFO: Major steps (console + log file)
- WARNING: Issues found (console + log file)
- ERROR: Failures (console + log file)

**Log file retention:**
- Daily files kept for 30 days (configurable)
- One file per day (all operations appended)
- Full stack traces on errors

</details>

[↑ Back to TOC](#table-of-contents)

---

## Common Workflows

<details>
<summary><strong>Click to expand common usage scenarios</strong></summary>

### Daily Operation

```bash
# Run at start of day to process overnight files
python main.py sync

# Check status
python main.py status
```

**What happens:**
1. Detects new incremental files in `data/reports/0031/`
2. Processes all unapplied incrementals in batch
3. Merges with existing database
4. Generates validation + change reports
5. Archives processed files
6. Updates state file

### Manual Processing

```bash
# Force full rebuild (ignore state)
python main.py sync --force  # (if force option added)

# Or manually delete state file to force rebuild
rm data/database/parquet_state.json
python main.py sync
```

### Weekly Full Refresh

1. Place weekly full backup file in `data/reports/0031/`
   - Filename: `0031-Contract Item Price Cat Pkg Extract 1108.xlsx` (MMDD format)
2. Run: `python main.py sync`
3. System auto-detects weekly full
4. Database completely refreshed
5. Incremental tracking reset

### Troubleshooting

**Check status:**
```bash
python main.py status
```

**Review logs:**
```bash
# Today's log
cat logs/pipeline_20251108.log

# Search for errors
grep "ERROR" logs/pipeline_20251108.log

# Check warnings
grep "WARNING" logs/pipeline_20251108.log
```

**Review reports:**
- Markdown: Quick overview in `data/database/audit/`
- Excel: Detailed analysis with 9 sheets

**Manual intervention:**
- Restore from backup: Copy from `data/database/backup/`
- Clear state: Delete `data/database/parquet_state.json`
- Re-process: Move files back from archive to `data/reports/0031/`

</details>

[↑ Back to TOC](#table-of-contents)

---

## Future Enhancements

<details>
<summary><strong>Click to expand planned improvements</strong></summary>

### Phase 1: Integration
- Process daily Allscripts files
- Enrich with 0031 database lookups
- Create `integrate_core/` if complexity grows

### Phase 2: Classification
- Classify records into buckets (update, create, links)
- Implement business rules
- Create `classify_core/` when logic is complex

### Phase 3: Export
- Generate final Excel exports
- Apply formatting and templates
- Create `export_core/` if export logic grows

### Performance
- Parallel processing for multiple files
- Incremental parquet writes (append mode)
- Caching for reference files

### Monitoring
- Email notifications on failures
- Metrics dashboard (processing times, error rates)
- Automated health checks

### Data Quality
- Additional validation rules
- Auto-correction for known issues
- Data quality scoring

</details>

[↑ Back to TOC](#table-of-contents)

---

## Glossary

<details>
<summary><strong>Click to expand terminology</strong></summary>

| Term | Description |
|------|-------------|
| **PMM Item Number** | Primary Material Master item identifier |
| **Corp Acct** | Corporate Account code (e.g., 0201, 0204) |
| **Vendor Code** | Vendor identifier |
| **Vendor Catalogue** | Vendor's catalog number for item |
| **Incremental File** | Daily update file with changes |
| **Weekly Full** | Complete database backup file |
| **Parquet** | Columnar storage format (efficient for analytics) |
| **State File** | JSON file tracking processing status |
| **Audit Folder** | Folder containing validation reports |
| **Archive Folder** | Folder for processed incremental files |
| **Backup Folder** | Folder for database backups |
| **5-Column Key** | Unique identifier using 5 fields |
| **Change Tracking** | Monitoring field-level changes between versions |
| **Batch Mode** | Processing multiple files in one operation |
| **Per-File Tracking** | Individual change tracking for each file |
| **Duplicate Analysis** | Finding items updated across multiple files |
| **Deduplication** | Removing duplicate records (keeping last) |
| **Mirror Accounts** | Paired corp accounts (0201↔0204, 0501↔0504) |

</details>

[↑ Back to TOC](#table-of-contents)

---

## Document Information

**Created:** 2025-11-08  
**Version:** 1.0  
**Author:** Data Pipeline Team  
**Project:** CLS Allscripts Data Processing Pipeline  
**Phase Coverage:** Phase 0 (Database Sync) only

**Related Documentation:**
- Configuration Guide: `config/config.json`
- User Manual: TBD
- API Reference: TBD

**Change Log:**
- 2025-11-08: Initial documentation for Phase 0

---

*End of Document*

[↑ Back to TOC](#table-of-contents)