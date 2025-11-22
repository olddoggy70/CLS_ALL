# Phase 0 (Sync) & Phase 1 (Integration) Architecture Documentation

**Project:** CLS Allscripts Data Processing Pipeline  
**Generated:** 2025-11-21  
**Version:** 2.1 (Consolidated & Detailed)  
**Python Version:** 3.12

---

## Table of Contents

1. [File Structure](#file-structure)
2. [Function Call Hierarchy](#function-call-hierarchy)
3. [Module Functions Summary](#module-functions-summary)
   - [Core Modules](#core-modules)
   - [Phase 0: Sync Modules](#phase-0-sync-modules)
   - [Phase 1: Integration Modules](#phase-1-integration-modules)
   - [Shared Utilities](#shared-utilities)
4. [Logger Hierarchy](#logger-hierarchy)
5. [Design Patterns](#design-patterns)
6. [Configuration](#configuration)
7. [Data Flow](#data-flow)
   - [Phase 0: Incremental Update](#phase-0-incremental-update-flow)
   - [Phase 0: Weekly Full Backup](#phase-0-weekly-full-backup-flow)
   - [Phase 1: Integration Flow](#phase-1-integration-flow)
8. [Key Concepts](#key-concepts)
9. [Performance Considerations](#performance-considerations)
10. [Error Handling](#error-handling)
11. [Common Workflows](#common-workflows)
12. [Future Enhancements](#future-enhancements)
13. [Glossary](#glossary)

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
│   ├── sync/                       # Phase 0: Database Sync
│   │   ├── __init__.py             # Exports process_sync, get_status
│   │   ├── orchestrator.py         # Main coordination logic
│   │   ├── processing.py           # Data processing operations
│   │   ├── quality.py              # Validation and change tracking
│   │   ├── reporting.py            # Report generation
│   │   ├── file_discovery.py       # File operations
│   │   ├── backup.py               # Backup utilities
│   │   └── sync_state.py           # State management
│   ├── integrate/                  # Phase 1: Integration
│   │   ├── __init__.py             # Exports process_integrate, get_status
│   │   └── pipeline.py             # Integration logic
│   ├── classify/                   # Phase 2: Classification
│   │   ├── __init__.py
│   │   └── pipeline.py
│   ├── export/                     # Phase 3: Export
│   │   ├── __init__.py
│   │   └── pipeline.py
│   └── utils/                      # Shared Utilities
│       ├── __init__.py
│       ├── date_utils.py
│       └── file_operations.py
├── tools/
│   └── optimize_schema.py          # Schema migration tool
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
    │   └── archive/               # Archived daily files
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
│   ├── setup_logging()                              [from src.logging_config]
│   │   └── Returns: (logger, log_file)
│   │
│   ├── [Command: sync]
│   │   └── process_sync()                           [from src.sync]
│   │       └── update_parquet_if_needed()           [from src.sync.orchestrator]
│   │           │
│   │           ├── load_state()                     [from src.sync.sync_state]
│   │           │
│   │           ├── get_weekly_full_files()          [from src.sync.file_discovery]
│   │           │
│   │           ├── get_incremental_files()          [from src.sync.file_discovery]
│   │           │
│   │           ├── [MODE 1: Weekly Full Backup Found]
│   │           │   └── process_weekly_full_backup()  [from src.sync.processing]
│   │           │       ├── create_backup()           [from src.sync.backup]
│   │           │       ├── clean_dataframe()         [internal - processing.py]
│   │           │       ├── convert_and_optimize_columns()  [internal - processing.py]
│   │           │       ├── validate_parquet_data()   [from src.sync.quality]
│   │           │       │   ├── Check Contract-Vendor relationship
│   │           │       │   ├── Check blank Vendor Catalogue
│   │           │       │   └── Check Vendor Catalogue consistency
│   │           │       └── cleanup_old_full_backups() [from src.sync.file_discovery]
│   │           │
│   │           ├── [MODE 2: Incremental Files Found]
│   │           │   └── apply_incremental_update()    [from src.sync.processing]
│   │           │       ├── create_backup()           [from src.sync.backup]
│   │           │       │   └── Creates timestamped .parquet backup
│   │           │       │
│   │           │       ├── Load incremental Excel file(s)
│   │           │       │   └── process_excel_files()     [internal - processing.py]
│   │           │       │       ├── Read Excel files
│   │           │       │       ├── Normalize string columns (strip)
│   │           │       │       └── Concatenate DataFrames
│   │           │       │
│   │           │       ├── clean_dataframe()         [internal - processing.py]
│   │           │       │   ├── Trim all string columns
│   │           │       │   ├── Convert blank strings to None
│   │           │       │   └── Trim column names
│   │           │       │
│   │           │       ├── convert_and_optimize_columns()  [internal - processing.py]
│   │           │       │   ├── Convert date columns (multiple formats)
│   │           │       │   ├── Optimize integer columns
│   │           │       │   └── Optimize float columns
│   │           │       │
│   │           │       ├── [BATCH MODE: Multiple files]
│   │           │       │   ├── Track changes per file
│   │           │       │   │   └── track_row_changes()   [from src.sync.quality]
│   │           │       │   │       ├── Create unique keys (5-column)
│   │           │       │   │       ├── Identify new vs updated rows
│   │           │       │   │       ├── Compare field-level changes
│   │           │       │   │       └── Generate date breakdown
│   │           │       │   │
│   │           │       │   ├── Analyze duplicate items across files
│   │           │       │   │   ├── Group by unique key
│   │           │       │   │   ├── Find items updated multiple times
│   │           │       │   │   └── Create duplicate analysis DataFrames
│   │           │       │   │
│   │           │       │   └── Aggregate all change results
│   │           │       │       ├── Combine changes from all files
│   │           │       │       ├── Create per-file summary
│   │           │       │       └── Calculate totals
│   │           │       │
│   │           │       ├── Deduplicate merged data
│   │           │       │   ├── Sort by Item Update Date
│   │           │       │   └── Keep last occurrence (unique by 5-col key)
│   │           │       │
│   │           │       ├── Merge with database
│   │           │       │   ├── Create merge keys
│   │           │       │   ├── Identify updates vs new records
│   │           │       │   ├── Remove old versions
│   │           │       │   └── Add all incremental data
│   │           │       │
│   │           │       ├── Write updated parquet
│   │           │       │
│   │           │       ├── validate_parquet_data()   [from src.sync.quality]
│   │           │       │   ├── Load permitted blank VPN list
│   │           │       │   ├── Check Contract-Vendor relationship
│   │           │       │   ├── Check blank Vendor Catalogue
│   │           │       │   └── Check Vendor Catalogue consistency
│   │           │       │
│   │           │       └── archive_file()            [from src.utils.file_operations]
│   │           │           ├── Move file to archive folder
│   │           │           └── Add timestamp if collision
│   │           │
│   │           ├── [MODE 3: Fallback - Full Rebuild]
│   │           │   ├── check_for_changes()          [from src.sync.file_discovery]
│   │           │   │   ├── get_excel_files()         [from src.sync.file_discovery]
│   │           │   │   ├── load_state()              [from src.sync.sync_state]
│   │           │   │   ├── Compare file timestamps
│   │           │   │   └── Detect new/modified/deleted files
│   │           │   │
│   │           │   ├── create_backup()               [from src.sync.backup]
│   │           │   │
│   │           │   ├── rebuild_parquet()             [from src.sync.processing]
│   │           │   │   ├── get_excel_files()         [from src.sync.file_discovery]
│   │           │   │   ├── process_excel_files()     [internal - processing.py]
│   │           │   │   ├── clean_dataframe()         [internal - processing.py]
│   │           │   │   ├── convert_and_optimize_columns()  [internal - processing.py]
│   │           │   │   └── validate_parquet_data()   [from src.sync.quality]
│   │           │   │
│   │           │   └── track_row_changes()           [from src.sync.quality]
│   │           │
│   │           ├── save_combined_report()            [from src.sync.reporting]
│   │           │   ├── generate_markdown_report()    [internal - reporting.py]
│   │           │   │   ├── Validation summary
│   │           │   │   ├── Change summary
│   │           │   │   ├── Per-file breakdown
│   │           │   │   └── Duplicate items summary
│   │           │   │
│   │           │   └── save_excel_report()           [internal - reporting.py]
│   │           │       └── _write_dataframe_to_worksheet()  [internal - reporting.py]
│   │           │           ├── Sheet 1: Summary
│   │           │           ├── Sheet 2: Per-File Summary
│   │           │           ├── Sheet 3: Date Breakdown
│   │           │           ├── Sheet 4: All Changes
│   │           │           ├── Sheet 5: New Rows
│   │           │           ├── Sheet 6: Updated Rows
│   │           │           ├── Sheet 7: Duplicate Items - Summary
│   │           │           ├── Sheet 8: Duplicate Items - All Versions
│   │           │           └── Sheet 9: Validation Issues
│   │           │
│   │           ├── save_state()                      [from src.sync.sync_state]
│   │           │   └── Save to parquet_state.json
│   │           │
│   │           ├── cleanup_old_backups()             [from src.sync.backup]
│   │           │   ├── Find backups older than retention period
│   │           │   └── Delete old backups
│   │           │
│   │           └── cleanup_old_archives()            [from src.utils.file_operations]
│   │               ├── Find archives older than retention period
│   │               └── Delete old archives
│   │
│   └── [Command: integrate]
│       └── process_integrate()                      [from src.integrate]
│           ├── _process_daily_data()
│           │   ├── Read Excel files
│           │   └── Convert date columns
│           │
│           ├── _prepare_database_dataframe()
│           │   ├── Load Parquet (scan + filters)
│           │   └── Remove mirrored pairs
│           │
│           ├── _create_lookup_tables()
│           │   └── Build 6 lookup tables
│           │
│           ├── _enrich_daily_data()
│           │   ├── Join PMM mappings
│           │   ├── Collapse PMM candidates
│           │   └── Join vendor info
│           │
│           ├── _add_contract_analysis()
│           ├── _add_reference_mappings()
│           ├── _add_highest_uom_price()
│           ├── _finalize_dataframe()
│           │
│           ├── _save_integrated_output()
│           │   └── extract_date_range()             [from src.utils.date_utils]
│           │
│           └── _archive_daily_files()
│               └── archive_file()                   [from src.utils.file_operations]
```

</details>

[↑ Back to TOC](#table-of-contents)

---

## Module Functions Summary

### Core Modules

#### main.py
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

**CLI Commands:**
- `python main.py all` - Run all phases
- `python main.py sync` - Phase 0: Database sync
- `python main.py integrate` - Phase 1: Integration
- `python main.py classify` - Phase 2: Classification
- `python main.py export` - Phase 3: Export
- `python main.py status` - Show pipeline status

**Logger:** `data_pipeline` (root logger)

</details>

#### src/logging_config.py
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

</details>

---

### Phase 0: Sync Modules

#### src/sync/orchestrator.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Main coordination logic for parquet update operations

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `update_parquet_if_needed()` | Main orchestration logic | `config: dict, paths: dict, force_rebuild: bool, incremental_file: Path \| None, logger: Logger \| None` | `bool` |
| `_clean_change_summary_for_state()` | Clean summary for JSON | `change_summary: dict \| None` | `dict \| None` |

**Processing Modes:**
1. **Weekly Full Mode:** New weekly full backup detected
2. **Incremental Mode:** Daily incremental files found
3. **Fallback Mode:** Full rebuild from all Excel files

**Logger:** `data_pipeline.sync.orchestrator`

</details>

#### src/sync/processing.py
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

**Key Features:**
- **Batch processing:** Handles single or multiple incremental files efficiently
- **Per-file tracking:** Tracks changes from each file separately in batch mode
- **Duplicate detection:** Finds items updated across multiple files
- **5-column unique key:** `PMM Item Number + Corp Acct + Vendor Code + Additional Cost Centre + Additional GL Account`
- **Deduplication:** Keeps last occurrence based on `Item Update Date`
- **Data optimization:** Date conversion, integer/float optimization, categorical types

**Logger:** `data_pipeline.sync.processing`

</details>

#### src/sync/quality.py
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

**Logger:** `data_pipeline.sync.quality`

</details>

#### src/sync/reporting.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Generate validation and change tracking reports

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `save_combined_report()` | Save validation + change reports | `validation_results: dict, change_results: dict, processing_time: float, audit_folder: Path, logger: Logger \| None` | `dict` |
| `generate_markdown_report()` | Generate markdown report | `validation_results: dict, change_results: dict, processing_time: float` | `str` |
| `save_excel_report()` | Save Excel report | `excel_file: Path, validation_results: dict, change_results: dict, logger: Logger \| None` | None |

**Report Files:**
1. **Markdown:** `validation_and_changes_report_YYYY-MM-DD.md`
2. **Excel:** `validation_and_changes_report_YYYY-MM-DD.xlsx`

**Logger:** `data_pipeline.sync.reporting`

</details>

#### src/sync/file_discovery.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** File finding and management

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `get_excel_files()` | Get all Excel files | `main_folder: Path, logger: Logger \| None` | `list[Path]` |
| `get_incremental_files()` | Get incremental files | `main_folder: Path, config: dict, logger: Logger \| None` | `list[Path]` |
| `get_weekly_full_files()` | Get weekly full files | `main_folder: Path, config: dict, logger: Logger \| None` | `list[Path]` |
| `check_for_changes()` | Check for file changes | `main_folder: Path, state_file: Path, logger: Logger \| None` | `tuple[bool, dict]` |
| `cleanup_old_full_backups()` | Remove old full backups | `reports_folder: Path, current_full_file: Path, config: dict, logger: Logger \| None` | None |

**Logger:** `data_pipeline.sync.files`

</details>

#### src/sync/backup.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Backup creation and cleanup

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `create_backup()` | Create timestamped backup | `db_file: Path, backup_folder: Path, logger: Logger \| None` | `Path \| None` |
| `cleanup_old_backups()` | Remove old backups | `backup_folder: Path, retention_days: int, logger: Logger \| None` | None |

**Backup Naming:** `0031_backup_YYYYMMDD_HHMMSS.parquet`

**Logger:** `data_pipeline.sync.backup`

</details>

#### src/sync/sync_state.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** State management and status tracking

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `load_state()` | Load state from JSON | `state_file: Path` | `dict` |
| `save_state()` | Save state to JSON | `state_file: Path, state: dict` | None |
| `get_update_status()` | Get current status | `config: dict, paths: dict` | `dict` |

**State File:** `data/database/parquet_state.json`

**Logger:** `data_pipeline.sync.state`

</details>

---

### Phase 1: Integration Modules

#### src/integrate/pipeline.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Enriches daily activity files with data from the 0031 baseline database

**Main Function:**
```python
process_integrate(config: dict, paths: dict, logger: Logger | None = None) -> bool
```

**Internal Functions:**

| Function | Description |
|----------|-------------|
| `_process_daily_data()` | Loads `Allscripts_historical_*.xlsx`, adds `Source_File` and `Index` columns, converts dates. |
| `_prepare_database_dataframe()` | Loads `0031.parquet`, filters out "PM" items and "ZZZ" vendors, handles mirror accounts (0201/0204). |
| `_create_lookup_tables()` | Creates 6 lookup tables for PMM mapping, descriptions, contracts, and vendor details. |
| `_enrich_daily_data()` | Joins daily data with lookup tables. Handles PMM candidate collapsing (DPN/MPN logic). |
| `_collapse_pmm_candidates()` | Consolidates rows where DPN and MPN map to the same PMM; expands if different. |
| `_add_contract_analysis()` | Checks if contract dates match the database; flags "New Contract" or mismatches. |
| `_add_reference_mappings()` | Maps Manufacturer and Vendor numbers using external reference files. |
| `_add_highest_uom_price()` | Calculates the highest price per base unit across all UOM levels (Base, AUOM1, AUOM2, AUOM3). |
| `_finalize_dataframe()` | Renames columns to match schema, adds duplicate flags. |
| `_save_integrated_output()` | Saves output to Parquet/XLSX with smart filename based on date range. |
| `_archive_daily_files()` | Moves processed daily files to the archive folder. |

**Logger:** `data_pipeline.integrate`

</details>

---

### Shared Utilities

#### src/utils/date_utils.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Common date manipulation functions.

- `extract_date_range(df, date_col, date_fmt)`: Finds min/max dates in a column for filename generation.

</details>

#### src/utils/file_operations.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Common file handling functions.

- `archive_file(file_path, archive_folder)`: Moves a file to archive, appending a timestamp if the file already exists.
- `cleanup_old_archives(archive_folder, retention_days)`: Deletes files older than the retention period.

</details>

[↑ Back to TOC](#table-of-contents)

---

## Logger Hierarchy

```
data_pipeline                              # Root logger (main.py)
│
├── data_pipeline.sync                     # Phase 0
│   ├── data_pipeline.sync.orchestrator
│   ├── data_pipeline.sync.processing
│   ├── data_pipeline.sync.quality
│   ├── data_pipeline.sync.reporting
│   ├── data_pipeline.sync.files
│   ├── data_pipeline.sync.backup
│   └── data_pipeline.sync.state
│
├── data_pipeline.integrate                # Phase 1
├── data_pipeline.classify                 # Phase 2
└── data_pipeline.export                   # Phase 3
```

**Logger Propagation:**
- Each module creates its own logger: `logging.getLogger('data_pipeline.module.submodule')`
- Loggers inherit handlers from parent `data_pipeline` logger
- Messages are formatted with module name: `%(name)s`

[↑ Back to TOC](#table-of-contents)

---

## Design Patterns

<details>
<summary><strong>Click to expand</strong></summary>

### 1. **Package-Based Architecture**
- Clean separation of phases into `src/sync`, `src/integrate`, etc.
- Shared code in `src/utils`.
- `__init__.py` files expose public APIs.

### 2. **Orchestrator Pattern**
- `orchestrator.py` (Phase 0) and `pipeline.py` (Phase 1) coordinate all operations.
- Delegates specific tasks to specialized modules.
- Handles multiple processing modes.

### 3. **State Management Pattern**
- `sync_state.py` manages persistent state in JSON.
- Tracks applied incrementals, timestamps, and metadata.
- Enables incremental processing and change detection.

### 4. **Batch Processing Pattern**
- Phase 0 handles both single and multiple files.
- Per-file change tracking in batch mode.
- Efficient: loads all files, processes once, writes once.

### 5. **Lookup Table Pattern (Phase 1)**
- Pre-builds lookups from database for fast joins.
- Avoids repeated large joins.
- 6 optimized tables: DPN map, MPN map, Descriptions, Contracts, Vendor Seq, Vendor Details.

### 6. **Smart Filename Pattern**
- Output filenames reflect the data range (e.g., `integrated_2023-10-01_to_2023-10-31.parquet`).
- Uses `extract_date_range` utility.

### 7. **Configuration-Driven Pattern**
- All paths, patterns, and settings in `config.json`.
- File patterns defined with glob expressions.
- Processing options (schema inference, type optimization) configurable.

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
  },
  "daily_files": {
    "pattern": "Allscripts_historical_*.xlsx",
    "archive_after_processing": true
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
    "float32_columns": ["Default UOM Price", "Price1", "Price2", "Price3", "Purchase UOM Price"],
    "categorical_columns": ["UOM1", "UOM2", "UOM3", "Corp Acct"]
  },
  "columns_to_drop": [
    "Corporation Info",
    "Item Status",
    "Commodity Code"
  ]
}
```

### Phase 1 Settings

```json
"phases": {
  "integration": {
    "enabled": true,
    "output_format": "parquet",
    "date_column": "Date and Time Stamp",
    "date_format": "%Y-%b-%d %I:%M:%S %p",
    "filename_prefix": "integrated"
  }
}
```

</details>

[↑ Back to TOC](#table-of-contents)

---

## Data Flow

<details>
<summary><strong>Click to expand data flow diagrams</strong></summary>

### Phase 0: Incremental Update Flow

```
Excel Files (Incremental)
    │
    ├─→ Read & Load (Batch)
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

### Phase 0: Weekly Full Backup Flow

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

### Phase 1: Integration Flow

```
Daily Files (Allscripts_historical_*.xlsx)
    │
    ├─→ Load & Process
    │       ├─→ Add Source_File & Index
    │       └─→ Convert dates
    │
    ├─→ Load Database (0031.parquet)
    │       ├─→ Filter PM items & ZZZ vendors
    │       └─→ Remove mirrored accounts
    │
    ├─→ Enrich Data
    │       ├─→ Join PMM mappings (DPN/MPN)
    │       ├─→ Join Vendor & Contract info
    │       └─→ Add Reference Mappings (MFN/VN)
    │
    ├─→ Business Logic
    │       ├─→ Contract Header Check
    │       └─→ Calculate Highest UOM Price
    │
    └─→ Save Output
            ├─→ Generate smart filename (date range)
            └─→ Write Parquet/XLSX
```

</details>

[↑ Back to TOC](#table-of-contents)

---

## Key Concepts

<details>
<summary><strong>Click to expand key concepts</strong></summary>

### Unique Key (Phase 0)
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

### Mirror Account Handling (Phase 1)
- **Pairs:** 0201 ↔ 0204, 0501 ↔ 0504.
- **Rule:** If an item exists in both accounts of a pair, keep the primary (0201/0501) and remove the secondary (0204/0504).
- **Goal:** Prevent duplicate records for the same logical item.

### PMM Candidate Collapse (Phase 1)
**Scenario:** A daily row has both a Distributor Part Number (DPN) and Manufacturer Part Number (MPN).
1. **Same PMM:** If both map to `PMM001` → Single row, `PMM_source="DPN,MPN"`.
2. **Different PMMs:** If DPN maps to `PMM001` and MPN maps to `PMM002` → Two rows created.
   - Row 1: `PMM001` (`PMM_source="DPN"`)
   - Row 2: `PMM002` (`PMM_source="MPN"`)
   - **Flag:** `Duplicates="Y"` added to output.

### Highest UOM Price (Phase 1)
Calculates the maximum price per base unit across all packaging levels:
- Normalizes `Purchase UOM Price` to a per-unit base price.
- Extrapolates to `AUOM1`, `AUOM2`, `AUOM3`.
- Takes the maximum value.

### Batch Processing (Phase 0)
When multiple incremental files are found:
1. **Load all files** into memory (concatenate)
2. **Track changes per file** (maintains source file information)
3. **Analyze duplicates** across files (items updated multiple times)
4. **Deduplicate** before merge (keep last occurrence by date)
5. **Merge once** with database (efficient single write)
6. **Archive all files** at once

</details>

[↑ Back to TOC](#table-of-contents)

---

## Performance Considerations

<details>
<summary><strong>Click to expand performance tips</strong></summary>

### Phase 0 Performance
- **Speed:** ~15-20 seconds per incremental file.
- **Batch Mode:** 3-4x faster than processing files individually.
- **Bottleneck:** Excel file reading.

### Phase 1 Performance
- **Speed:** ~10-15 seconds total for daily run.
- **Optimization:**
  - `scan_parquet` with filters avoids loading unused data.
  - **Float32** columns reduce memory usage by 50% for numeric data.
  - Pre-built lookup tables minimize join overhead.

### Memory Management
- **Peak Usage:** ~850MB (Phase 1).
- **Strategy:** Use `Float32` for high-cardinality numeric columns (configured in `type_optimization`).
- **Lazy Loading:** Use Polars lazy API where possible.

### Disk I/O
- **Read:** Database parquet (~50MB), Incremental Excel (~5-10MB).
- **Write:** Updated parquet (~50MB), Backup (~50MB), Reports.
- **Recommendation:** Use SSD for data folder.

</details>

[↑ Back to TOC](#table-of-contents)

---

## Error Handling

<details>
<summary><strong>Click to expand error handling strategies</strong></summary>

### Phase 0 Strategies
- **Backup First:** Always creates a timestamped backup of `0031.parquet` before writing.
- **Atomic State:** `parquet_state.json` only updated on success.
- **Validation:** Generates reports for data issues (e.g., multiple vendors per contract) without stopping pipeline.
- **Automatic Recovery:** If processing fails, the database is not corrupted (backup exists).

### Phase 1 Strategies
- **Missing Files:** Raises `ValueError` if no daily files found.
- **Date Parsing:** Uses `strict=False` to handle malformed dates (converts to Null).
- **Safe Archival:** Checks for filename collisions before moving processed files.
- **No Partial Writes:** Output file is written only after full successful processing.

### Validation Checks
- **Contract-Vendor:** Ensures one contract = one vendor.
- **Blank Catalogue:** Checks for unexpected blank values.
- **Catalogue Consistency:** Same PMM+Vendor+CorpAcct should have same catalogue.

</details>

[↑ Back to TOC](#table-of-contents)

---

## Common Workflows

<details>
<summary><strong>Click to expand common usage scenarios</strong></summary>

### Daily Operation
```bash
# 1. Sync Database (Phase 0)
python main.py sync

# 2. Integrate Daily Files (Phase 1)
python main.py integrate

# 3. Check Status
python main.py status
```

### Full Pipeline Run
```bash
python main.py all
```

### Manual Rebuild
```bash
# Force full rebuild of database from all historical Excel files
python main.py sync --force
```

### Troubleshooting
- **Check Status:** `python main.py status`
- **Review Logs:** `cat logs/pipeline_YYYYMMDD.log`
- **Review Reports:** Check `data/database/audit/` for Markdown/Excel reports.
- **Restore Backup:** Copy from `data/database/backup/` if needed.

</details>

[↑ Back to TOC](#table-of-contents)

---

## Future Enhancements

<details>
<summary><strong>Click to expand planned improvements</strong></summary>

- **Phase 2 (Classification):** Implement business rules to bucket records (Update, Create, Link).
- **Phase 3 (Export):** Generate final formatted Excel outputs.
- **Parallel Processing:** Use `multiprocessing` for reading Excel files.
- **Incremental Parquet:** Use append mode for faster writes (requires careful deduplication).
- **Monitoring:** Email notifications on failures.
- **Data Quality:** Additional validation rules and auto-correction.

</details>

[↑ Back to TOC](#table-of-contents)

---

## Glossary

<details>
<summary><strong>Click to expand terminology</strong></summary>

| Term | Description |
|------|-------------|
| **PMM Item Number** | Primary Material Master item identifier. |
| **DPN / MPN** | Distributor / Manufacturer Part Number. |
| **Incremental File** | Daily update file for Phase 0. |
| **Daily File** | Daily activity file for Phase 1. |
| **Mirror Accounts** | Paired accounts (0201/0204) representing same inventory. |
| **Parquet** | Columnar storage format used for the database. |
| **5-Column Key** | Unique composite key for identifying records. |
| **State File** | JSON file tracking processing status. |
| **Audit Folder** | Folder containing validation reports. |
| **Archive Folder** | Folder for processed files. |
| **Backup Folder** | Folder for database backups. |

</details>

[↑ Back to TOC](#table-of-contents)

---

## Document Information

**Created:** 2025-11-08 (Phase 0) | 2025-11-21 (Phase 1 Added)  
**Version:** 2.1 (Consolidated & Detailed)  
**Author:** Data Pipeline Team  
**Phase Coverage:** Phase 0 (Database Sync) + Phase 1 (Integration)

[↑ Back to TOC](#table-of-contents)