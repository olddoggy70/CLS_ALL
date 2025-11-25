# Phase 0 (Sync) & Phase 1 (Integration) Architecture Documentation

**Project:** CLS Allscripts Data Processing Pipeline  
**Generated:** 2025-11-22 20:38:00  
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
├── src/
│   ├── __init__.py
│   ├── logging_config.py           # Logging setup
│   ├── constants.py                # Global constants
│   ├── sync/                       # Phase 0: Database Sync
│   │   ├── __init__.py             # Exports process_sync, get_status
│   │   ├── orchestrator.py         # Main coordination logic
│   │   ├── core.py                 # Core processing logic
│   │   ├── transformation.py       # Data cleaning & optimization
│   │   ├── ingest.py               # File reading
│   │   ├── merge.py                # Merge & deduplication logic
│   │   ├── quality.py              # Validation and change tracking
│   │   ├── reporting.py            # Report generation
│   │   ├── file_discovery.py       # File operations
│   │   ├── backup.py               # Backup utilities
│   │   └── sync_state.py           # State management
│   ├── integrate/                  # Phase 1: Integration
│   │   ├── __init__.py             # Exports process_integrate, get_status
│   │   ├── pipeline.py             # Integration orchestration
│   │   ├── ingest.py               # Daily file reading
│   │   ├── baseline.py             # Database preparation
│   │   └── enrichment.py           # Data enrichment logic
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
    └── output/
        ├── integrated/            # Phase 1 output
        ├── classified/            # Phase 2 output
        └── exports/               # Phase 3 output
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
│   │           ├── apply_incremental_update()       [from src.sync.core]
│   │           │   ├── process_excel_files()        [from src.sync.ingest]
│   │           │   ├── clean_dataframe()            [from src.sync.transformation]
│   │           │   ├── convert_and_optimize_columns() [from src.sync.transformation]
│   │           │   ├── deduplicate_data()           [from src.sync.merge]
│   │           │   ├── prepare_merge_keys()         [from src.sync.merge]
│   │           │   ├── identify_changes()           [from src.sync.merge]
│   │           │   ├── merge_dataframes()           [from src.sync.merge]
│   │           │   ├── validate_parquet_data()      [from src.sync.quality]
│   │           │   └── track_row_changes()          [from src.sync.quality]
│   │           │       ├── Change summary
│   │           │       ├── Per-file breakdown
│   │           │       └── Duplicate items summary
│   │           │
│   │           ├── save_combined_report()           [from src.sync.reporting]
│   │           │   ├── generate_markdown_report()
│   │           │   └── save_excel_report()
│   │           │       └── _write_dataframe_to_worksheet()
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
│   │           ├── create_backup()                   [from src.sync.backup]
│   │           │   └── Create timestamped backup
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
│           ├── process_daily_data()                 [from src.integrate.ingest]
│           │   ├── Read Excel files
│           │   └── Convert date columns
│           │
│           ├── prepare_database_dataframe()         [from src.integrate.baseline]
│           │   ├── Load Parquet (scan + filters)
│           │   └── Remove mirrored pairs
│           │
│           ├── create_lookup_tables()               [from src.integrate.baseline]
│           │   └── Build 6 lookup tables
│           │
│           ├── enrich_daily_data()                  [from src.integrate.enrichment]
│           │   ├── Join PMM mappings
│           │   ├── Collapse PMM candidates
│           │   └── Join vendor info
│           │
│           ├── add_contract_analysis()              [from src.integrate.enrichment]
│           ├── add_reference_mappings()             [from src.integrate.enrichment]
│           ├── add_highest_uom_price()              [from src.integrate.enrichment]
│           ├── _finalize_dataframe()                [internal]
│           │
│           ├── _save_integrated_output()            [internal]
│           │   └── extract_date_range()             [from src.utils.date_utils]
│           │
│           └── _archive_daily_files()               [internal]
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

#### src/sync/core.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Core processing logic - incremental updates and rebuilds

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `apply_incremental_update()` | Apply incremental updates | `db_file: Path, incremental_files: Path \| list[Path], config: dict, backup_folder: Path, audit_folder: Path, archive_folder: Path \| None, blank_vpn_permitted_file: Path \| None, logger: Logger \| None` | `tuple[DataFrame, dict, dict]` |
| `rebuild_parquet()` | Full rebuild from Excel | `main_folder: Path, db_file: Path, config: dict, skip_cleaning: bool, blank_vpn_permitted_file: Path \| None, logger: Logger \| None` | `tuple[DataFrame, dict]` |
| `process_weekly_full_backup()` | Process weekly full backup | `weekly_file: Path, config: dict, paths: dict, logger: Logger \| None` | `tuple[DataFrame, dict]` |

**Logger:** `data_pipeline.sync`

</details>

#### src/sync/transformation.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Data cleaning, type conversion, and optimization

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `clean_dataframe()` | Clean and normalize data | `df: DataFrame, logger: Logger \| None` | `DataFrame` |
| `convert_and_optimize_columns()` | Convert dates and optimize types | `df: DataFrame, config: dict, logger: Logger \| None` | `DataFrame` |
| `apply_categorical_types()` | Apply categorical types | `df: DataFrame, config: dict` | `DataFrame` |
| `apply_filters()` | Apply row filtering | `df: DataFrame, config: dict, logger: Logger \| None` | `DataFrame` |

**Schema Enforcement:**
- Uses `Schema0031` definition for strict type enforcement
- Explicit casting to prevent type mismatch errors
- Optimizes column types (Float32, categorical)

**Logger:** `data_pipeline.sync`

</details>

#### src/sync/ingest.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** File reading and ingestion

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `process_excel_files()` | Load and concat Excel files | `file_paths: list[Path], infer_schema_length: int, logger: Logger \| None` | `DataFrame \| None` |

**Logger:** `data_pipeline.sync.ingest`

</details>

#### src/sync/merge.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Merge logic and deduplication

**Functions:**

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `deduplicate_data()` | Deduplicate based on 5-col key | `df: DataFrame, unique_keys: list[str], logger: Logger \| None` | `tuple[DataFrame, int]` |
| `prepare_merge_keys()` | Create merge keys | `df: DataFrame, logger: Logger \| None` | `DataFrame` |
| `identify_changes()` | Identify new vs updated rows | `current_df: DataFrame, new_df: DataFrame, logger: Logger \| None` | `tuple[list, list]` |
| `merge_dataframes()` | Apply merge | `current_df: DataFrame, new_df: DataFrame, update_keys: list, logger: Logger \| None` | `DataFrame` |

**5-Column Unique Key:**
- PMM Item Number
- Vendor Catalogue
- Corp Acct
- Contract No
- Vendor Code

**Logger:** `data_pipeline.sync.merge`

</details>

#### src/sync/quality.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Data validation and change tracking

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

**Excel Report Sheets:**
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

**State Contents:**
- Last processed files
- Processing timestamps
- Change summaries
- File checksums

**Logger:** `data_pipeline.sync.state`

</details>

---

### Phase 1: Integration Modules

#### src/integrate/pipeline.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Orchestrates the integration process

**Main Function:**
```python
process_integrate(config: dict, paths: dict, logger: Logger | None = None) -> bool
```

**Internal Functions:**

| Function | Description |
|----------|-------------|
| `_finalize_dataframe()` | Renames columns to match schema, adds duplicate flags |
| `_save_integrated_output()` | Saves output to Parquet/XLSX with smart filename |
| `_archive_daily_files()` | Moves processed daily files to archive |

**Logger:** `data_pipeline.integrate`

</details>

#### src/integrate/ingest.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Handles loading and initial processing of daily Excel files

**Functions:**

| Function | Description |
|----------|-------------|
| `process_daily_data()` | Loads daily Excel files, adds source tracking, converts dates |
| `_convert_date_columns()` | Converts string columns to date format using multiple patterns |

**Features:**
- Adds `Source_File` column for tracking
- Adds row index per file
- Handles multiple date formats

**Logger:** `data_pipeline.integrate.ingest`

</details>

#### src/integrate/baseline.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Prepares the 0031 database and creates lookup tables

**Functions:**

| Function | Description |
|----------|-------------|
| `prepare_database_dataframe()` | Loads 0031.parquet, filters data, handles mirror accounts |
| `create_lookup_tables()` | Creates 6 lookup tables for PMM mapping, contracts, etc. |

**6 Lookup Tables:**
1. PMM mapping by Distributor Part Number (DPN)
2. PMM mapping by Manufacturer Part Number (MPN)
3. PMM to Description
4. Contract database
5. Vendor sequence lookup
6. Vendor detail lookup

**Mirror Account Handling:**
- Maps: 0201↔0204, 0501↔0504
- Removes duplicate 0204/0504 entries from mirrored pairs

**Logger:** `data_pipeline.integrate.baseline`

</details>

#### src/integrate/enrichment.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Applies business logic and enriches daily data

**Functions:**

| Function | Description |
|----------|-------------|
| `enrich_daily_data()` | Joins daily data with lookup tables, collapses PMM candidates |
| `_collapse_pmm_candidates()` | Consolidates rows where DPN and MPN map to same PMM |
| `add_contract_analysis()` | Checks if contract dates match the database |
| `add_reference_mappings()` | Maps Manufacturer and Vendor numbers using external files |
| `add_highest_uom_price()` | Calculates highest price per base unit across all UOM levels |

**PMM Candidate Logic:**
- If DPN and MPN both map to same PMM: consolidate to single row
- If they map to different PMMs: expand to multiple rows
- Tracks source (DPN, MPN, or both)

**Contract Analysis Flags:**
- "Date Matched" - Dates align perfectly
- "New Contract" - Not found in 0031
- "Start Date not Match" - Only start date differs
- "End Date not Match" - Only end date differs
- "Start & End Date not Match" - Both dates differ

**Logger:** `data_pipeline.integrate.enrichment`

</details>

---

### Shared Utilities

#### src/utils/date_utils.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Common date manipulation functions

**Functions:**

| Function | Description |
|----------|-------------|
| `extract_date_range()` | Finds min/max dates in column for filename generation |

</details>

#### src/utils/file_operations.py
<details>
<summary><strong>Click to expand</strong></summary>

**Purpose:** Common file handling functions

**Functions:**

| Function | Description |
|----------|-------------|
| `archive_file()` | Moves file to archive, appending timestamp if exists |
| `cleanup_old_archives()` | Deletes files older than retention period |

</details>

[↑ Back to TOC](#table-of-contents)

---

## Logger Hierarchy

```
data_pipeline                              # Root logger (main.py)
│
├── data_pipeline.sync                     # Phase 0
│   ├── data_pipeline.sync.orchestrator
│   ├── data_pipeline.sync.core
│   ├── data_pipeline.sync.transformation
│   ├── data_pipeline.sync.ingest
│   ├── data_pipeline.sync.merge
│   ├── data_pipeline.sync.quality
│   ├── data_pipeline.sync.reporting
│   ├── data_pipeline.sync.files
│   ├── data_pipeline.sync.backup
│   └── data_pipeline.sync.state
│
├── data_pipeline.integrate                # Phase 1
│   ├── data_pipeline.integrate.pipeline
│   ├── data_pipeline.integrate.ingest
│   ├── data_pipeline.integrate.baseline
│   └── data_pipeline.integrate.enrichment
│
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
- Shared code in `src/utils`
- `__init__.py` files expose public APIs

### 2. **Orchestrator Pattern**
- `orchestrator.py` (Phase 0) and `pipeline.py` (Phase 1) coordinate all operations
- Delegates specific tasks to specialized modules
- Handles multiple processing modes

### 3. **State Management Pattern**
- `sync_state.py` manages persistent state in JSON
- Tracks applied incrementals, timestamps, and metadata
- Enables incremental processing and change detection

### 4. **Batch Processing Pattern**
- Phase 0 handles both single and multiple files
- Per-file change tracking in batch mode
- Efficient: loads all files, processes once, writes once

### 5. **Lookup Table Pattern (Phase 1)**
- Pre-builds lookups from database for fast joins
- Avoids repeated large joins
- 6 optimized tables: DPN map, MPN map, Descriptions, Contracts, Vendor Seq, Vendor Details

### 6. **Smart Filename Pattern**
- Output filenames reflect data range (e.g., `integrated_2023-10-01_to_2023-10-31.parquet`)
- Uses `extract_date_range` utility

### 7. **Configuration-Driven Pattern**
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
    "float32_columns": ["Default UOM Price", "Price1", "Price2", "Price3"],
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
    │       ├─→ Apply schema enforcement
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
    │       ├─→ Apply updates using unique keys
    │       └─→ Concatenate new rows
    │
    ├─→ Validate
    │       ├─→ Contract-vendor relationship check
    │       ├─→ Blank VPN check (with permitted list)
    │       └─→ Vendor catalogue consistency
    │
    ├─→ Save
    │       ├─→ Write updated parquet
    │       ├─→ Create timestamped backup
    │       └─→ Save state to JSON
    │
    └─→ Report & Cleanup
            ├─→ Generate markdown report
            ├─→ Generate Excel report (9 sheets)
            ├─→ Archive processed files
            └─→ Cleanup old backups
```

### Phase 0: Weekly Full Backup Flow

```
Weekly Full File (MMDD.xlsx)
    │
    ├─→ Detect as weekly full (file pattern match)
    │
    ├─→ Process as Fresh Database
    │       ├─→ Load entire file
    │       ├─→ Clean & transform
    │       ├─→ Deduplicate
    │       └─→ Optimize types
    │
    ├─→ Replace Existing Database
    │       ├─→ Backup old database
    │       └─→ Write new database
    │
    └─→ Cleanup
            ├─→ Remove old weekly full files
            └─→ Archive current weekly file
```

### Phase 1: Integration Flow

```
Daily Files (Allscripts_historical_*.xlsx)
    │
    ├─→ Load Daily Data
    │       ├─→ Read Excel files
    │       ├─→ Add Source_File column
    │       ├─→ Add Index column
    │       └─→ Convert date columns
    │
    ├─→ Prepare Database
    │       ├─→ Load 0031.parquet
    │       ├─→ Filter out PM items
    │       ├─→ Filter out ZZZ vendors
    │       ├─→ Handle mirror accounts (0201/0204, 0501/0504)
    │       └─→ Prefix all columns with "0031_"
    │
    ├─→ Create Lookup Tables (6 tables)
    │       ├─→ PMM by DPN
    │       ├─→ PMM by MPN
    │       ├─→ PMM to Description
    │       ├─→ Contract database
    │       ├─→ Vendor sequence
    │       └─→ Vendor details
    │
    ├─→ Enrich Daily Data
    │       ├─→ Join PMM mappings (DPN & MPN)
    │       ├─→ Collapse PMM candidates
    │       │       ├─→ If DPN & MPN → same PMM: consolidate
    │       │       └─→ If DPN & MPN → diff PMM: expand
    │       ├─→ Join PMM description
    │       ├─→ Join vendor sequence list
    │       └─→ Join vendor details
    │
    ├─→ Add Business Logic
    │       ├─→ Contract analysis
    │       │       ├─→ Compare contract dates
    │       │       └─→ Flag matches/mismatches
    │       ├─→ Reference mappings
    │       │       ├─→ MFN mapping
    │       │       └─→ VN mapping
    │       └─→ Calculate highest UOM price
    │               ├─→ Cast quantities to Float32
    │               ├─→ Find purchase UOM qty
    │               ├─→ Calculate base price
    │               └─→ Find max across all UOMs
    │
    ├─→ Finalize
    │       ├─→ Rename PMM column
    │       └─→ Add duplicate flag
    │
    ├─→ Save Output
    │       ├─→ Extract date range from data
    │       └─→ Save as integrated_YYYY-MM-DD_to_YYYY-MM-DD.[format]
    │
    └─→ Cleanup
            └─→ Archive processed daily files
```

</details>

[↑ Back to TOC](#table-of-contents)

---

## Key Concepts

<details>
<summary><strong>Click to expand</strong></summary>

### 5-Column Unique Key
The pipeline uses a composite key to uniquely identify each record:
1. PMM Item Number
2. Vendor Catalogue
3. Corp Acct
4. Contract No
5. Vendor Code

**Usage:**
- Deduplication
- Change tracking
- Merge operations

### Mirror Accounts
Certain corporate accounts are mirrored:
- 0201 ↔ 0204
- 0501 ↔ 0504

**Phase 1 Handling:**
- Identifies mirrored pairs
- Removes 0204 and 0504 from pairs (keeps 0201 and 0501)
- Preserves standalone 0204/0504 records

### PMM Candidate Collapsing
When matching daily data to 0031:
- **Distributor Part Number (DPN)** may map to a PMM
- **Manufacturer Part Number (MPN)** may also map to a PMM
- If both map to **same PMM**: Consolidate to single row with source "DPN,MPN"
- If they map to **different PMMs**: Expand to multiple rows, each with its source

### Schema Enforcement (Schema0031)
Strict schema definition prevents type mismatches:
- Defines expected types for all columns
- Explicit casting during transformation
- Prevents "String incompatible with Float32" errors

### Incremental vs Full Processing
**Incremental:**
- Pattern: `*_YYYY_MM_DD*.xlsx`
- Merges with existing database
- Tracks changes
- Most common mode

**Weekly Full:**
- Pattern: `*MMDD.xlsx` (e.g., `0201.xlsx`)
- Replaces entire database
- Faster for large changes
- Auto-detected

</details>

[↑ Back to TOC](#table-of-contents)

---

## Performance Considerations

<details>
<summary><strong>Click to expand</strong></summary>

### Polars Usage
- High-performance DataFrame library
- Lazy evaluation with `.scan_parquet()`
- Efficient memory usage
- Fast column operations

### Type Optimization
- Float32 instead of Float64 where possible
- Categorical types for repeated values
- Reduces memory footprint by 30-50%

### Lookup Tables
- Pre-build 6 lookup tables once
- Reuse for all daily records
- Avoids repeated scans of 1.4M+ row database

### Batch Processing
- Process multiple incremental files at once
- Single write operation at the end
- Per-file change tracking maintained

### Lazy Loading
- Use `.scan_parquet()` with filters
- Only load necessary columns
- Reduces I/O and memory

### Schema Inference
- `infer_schema_length: 0` in config
- Reads entire file to infer types
- Prevents type conflicts

</details>

[↑ Back to TOC](#table-of-contents)

---

## Error Handling

<details>
<summary><strong>Click to expand</strong></summary>

### Common Errors

**SchemaError: type String incompatible with expected type Float32**
- **Cause:** Type mismatch between database and new data
- **Solution:** Schema enforcement with explicit casting
- **Prevention:** Use Schema0031 definition

**No incremental files found**
- **Cause:** Files already processed or wrong naming
- **Solution:** Check archive folder, verify file patterns

**Reference file not found**
- **Cause:** Missing MFN/VN mapping files
- **Solution:** Ensure files exist in `data/ref_files/`

### Validation Warnings
- Contract-vendor relationship violations
- Blank vendor catalogues (check against permitted list)
- Vendor catalogue inconsistencies

### Recovery
- Automatic backups before each update
- State file tracks processing history
- Can rebuild from full backup

</details>

[↑ Back to TOC](#table-of-contents)

---

## Common Workflows

<details>
<summary><strong>Click to expand</strong></summary>

### Daily Processing
```bash
python main.py integrate
```
- Syncs database
- Processes daily files
- Archives everything
- Generates reports

### Weekly Full Update
1. Place weekly full file in `data/reports/0031/`
2. Run: `python main.py sync`
3. Pipeline auto-detects and processes as full backup

### Manual Database Rebuild
```bash
# Delete state file
del data\database\parquet_state.json

# Ensure all incrementals are in reports folder
# Run sync
python main.py sync
```

### Check Status
```bash
python main.py status
```
Shows current state of all phases

### Full Pipeline
```bash
python main.py export
```
Runs all phases: Sync → Integrate → Classify → Export

</details>

[↑ Back to TOC](#table-of-contents)

---

## Future Enhancements

<details>
<summary><strong>Click to expand</strong></summary>

### Potential Improvements
1. **Parallel Processing:** Process multiple files concurrently
2. **Incremental Integration:** Only process new daily records
3. **Data Dictionary:** Document all columns and business rules
4. **Automated Testing:** Unit tests for critical functions
5. **Performance Monitoring:** Track processing times and memory usage
6. **Delta Lake:** Use Delta format for ACID transactions
7. **Cloud Storage:** Support for S3/Azure Blob storage
8. **API Interface:** REST API for triggering pipeline
9. **Notification System:** Email/Slack notifications on completion
10. **Data Lineage:** Track data provenance through pipeline

</details>

[↑ Back to TOC](#table-of-contents)

---

## Glossary

**0031:** The main database file containing contract item pricing data

**DPN:** Distributor Part Number - vendor's catalog number

**MPN:** Manufacturer Part Number - manufacturer's catalog number

**PMM:** Product Master Management item number - internal standardized ID

**Corp Acct:** Corporate Account - facility/location identifier

**Mirror Accounts:** Paired accounts (0201/0204, 0501/0504) representing same facility

**Incremental File:** Daily update file with changes since last backup

**Weekly Full:** Complete database snapshot, typically weekly

**UOM:** Unit of Measure (e.g., EA, BX, CS)

**VPN:** Vendor Part Number (synonym for DPN)

**Schema0031:** Strict schema definition for type enforcement

**5-Column Key:** Composite unique key: PMM + Vendor Catalogue + Corp Acct + Contract No + Vendor Code

[↑ Back to TOC](#table-of-contents)

---

**End of Documentation**