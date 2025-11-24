# CLS Allscripts Data Processing Pipeline

A multi-phase data processing pipeline for synchronizing, integrating, classifying, and exporting healthcare procurement data.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Pipeline Phases](#pipeline-phases)
- [Configuration](#configuration)
- [Common Workflows](#common-workflows)
- [Troubleshooting](#troubleshooting)
- [Architecture](#architecture)
- [Important Notes](#important-notes)

---

## Overview

This pipeline automates the processing of healthcare procurement data through four phases:

1. **Phase 0 (Sync)** - Synchronize the 0031 database from incremental backups
2. **Phase 1 (Integration)** - Integrate daily Allscripts files with the 0031 baseline
3. **Phase 2 (Classification)** - Classify records into processing buckets
4. **Phase 3 (Export)** - Generate final export files

The pipeline handles ~1.4M records with automatic change tracking, data validation, and duplicate detection.

---

## Features

### Phase 0: Database Sync
- ✅ **Automatic detection** of full backups and incrementals
- ✅ **Batch processing** - processes multiple files efficiently in one operation
- ✅ **Change tracking** - per-file change analysis with field-level detail
- ✅ **Duplicate detection** - identifies items updated across multiple files
- ✅ **Data validation** - contract-vendor relationships, blank catalogues, consistency checks
- ✅ **Automatic archiving** - processed files moved to archive with configurable retention
- ✅ **Backup & recovery** - automatic backups before every modification

### Phase 1: Integration
- ✅ **Enrichment** - adds PMM mappings, vendor information, contract analysis
- ✅ **Reference mappings** - MFN and VN mappings from reference files
- ✅ **Duplicate handling** - collapses multiple PMM candidates per record

### Phase 2: Classification
- 🚧 **In development** - classify records into update/create/link buckets

### Phase 3: Export
- ✅ **Excel generation** - formatted exports with date ranges
- ✅ **Automatic dating** - filename includes data date range

---

## Installation

### Prerequisites

- Python 3.12+
- Required packages: `polars`, `xlsxwriter`, `openpyxl`, `psutil`

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd cls_project
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the pipeline**
   - Edit `config/config.json` to match your environment
   - Update paths to match your data locations
   - Configure file patterns for your naming conventions

4. **Prepare reference files**
   - Place reference files in `data/ref_files/`:
     - `MFN Mapping.xlsx`
     - `VN Mapping.xlsx`
     - `Blank Vpn Permitted.xlsx`

---

## Quick Start

### First Time Setup

1. **Place baseline database**
   ```bash
   # Place your full backup file in data/reports/0031/
   # Example: 0031-Contract Item Price Cat Pkg Extract 1108.xlsx
   ```

2. **Initial sync**
   ```bash
   python main.py sync
   ```
   This creates the baseline `0031.parquet` database (~50MB, 1.4M rows).

### Daily Operations

```bash
# Process new incremental files (most common)
python main.py integrate

# Check current status
python main.py status

# Full pipeline (sync + integrate + classify + export)
python main.py all
```

---

## Usage

### Command Line Interface

```bash
python main.py <command>
```

**Available Commands:**

| Command | Description | Phases Run |
|---------|-------------|------------|
| `sync` | Update 0031 database only | Phase 0 |
| `integrate` | Sync database + integrate daily files | Phase 0 → 1 |
| `classify` | Full pipeline up to classification | Phase 0 → 1 → 2 |
| `export` | Complete end-to-end processing | Phase 0 → 1 → 2 → 3 |
| `all` | Same as export (full pipeline) | Phase 0 → 1 → 2 → 3 |
| `status` | Show current pipeline status | Status only |
| `--help` | Show help information | Help only |

### Examples

**Daily workflow - process overnight files:**
```bash
python main.py integrate
```

**Generate final exports:**
```bash
python main.py export
```

**Check what's been processed:**
```bash
python main.py status
```

**Update database only (no further processing):**
```bash
python main.py sync
```

---

## Pipeline Phases

### Phase 0: Database Sync

**Purpose:** Maintain the 0031.parquet database by applying incremental updates or full refreshes.

**Input Files:**
- **Full:** `0031-Contract Item Price Cat Pkg Extract 1108.xlsx` (MMDD format)
- **Incremental:** `0031-Contract Item Price Cat Pkg Extract 2025_11_06.xlsx`

**Processing:**
1. Auto-detects file type (full vs incremental)
2. Creates backup before modifications
3. Applies changes with field-level tracking
4. Validates data quality
5. Generates comprehensive reports
6. Archives processed files

**Outputs:**
- `data/database/0031.parquet` - Main database (~50MB)
- `data/database/backup/` - Timestamped backups
- `data/database/audit/` - Validation and change reports (Markdown + Excel)
- `data/reports/archive/` - Archived incremental files

**Key Features:**
- **Batch mode:** Process 5 files in ~20 seconds (vs ~100 seconds individually)
- **Duplicate detection:** Finds items updated across multiple files
- **5-column unique key:** `PMM Item Number + Corp Acct + Vendor Code + Additional Cost Centre + Additional GL Account`
- **Automatic deduplication:** Keeps last occurrence by `Item Update Date`

### Phase 1: Integration

**Purpose:** Integrate daily Allscripts files with the 0031 baseline database.

**Input Files:**
- `data/daily_files/Allscripts_historical_deduped_*.xlsx`

**Processing:**
1. Reads daily files with source tracking
2. Enriches with 0031 database lookups (PMM mappings, vendor info)
3. Adds contract analysis and reference mappings
4. Flags duplicates and inconsistencies

**Outputs:**
- `data/integrated/integrated_<timestamp>.parquet`

### Phase 2: Classification

**Purpose:** Classify records into processing buckets.

**Status:** 🚧 In development

**Planned Buckets:**
- Update - Records to update
- Create - New records to create
- Vendor Link - Vendor linkage updates
- Contract Link - Contract linkage updates

### Phase 3: Export

**Purpose:** Generate final Excel exports from processed data.

**Input:** Latest integrated file (until Phase 2 is implemented)

**Output:**
- `data/exports/ExportFile_<date_range>.xlsx`
- Example: `ExportFile_2025-11-01~2025-11-06.xlsx`

---

## Configuration

### Key Settings in `config/config.json`

**File Patterns:**
```json
"file_patterns": {
  "0031_incremental": {
    "pattern": "0031-Contract Item Price Cat Pkg Extract *.xlsx",
    "archive_after_processing": true
  },
  "0031_full": {
    "pattern": "0031-Contract Item Price Cat Pkg Extract [0-9][0-9][0-9][0-9].xlsx",
    "keep_only_latest": true
  }
}
```

**Processing Schedule:**
```json
"processing_schedule": {
  "process_incrementals_on_startup": true,
  "auto_detect_weekly_full": true,
  "max_incrementals_per_run": 10
}
```

**Archive Settings:**
```json
"archive_settings": {
  "enabled": true,
  "retention_days": 90
}
```

**Backup Settings:**
```json
"update_settings": {
  "backup_retention_days": 14
}
```

**Logging:**
```json
"logging": {
  "console_level": "INFO",
  "file_level": "DEBUG",
  "log_folder": "logs",
  "retention_days": 30
}
```

---

## Common Workflows

### Daily Morning Routine

```bash
# 1. Check status
python main.py status

# 2. Process overnight incrementals
python main.py integrate

# 3. Review reports
# Check: data/database/audit/validation_and_changes_report_<date>.xlsx
```

### Full Refresh

```bash
# 1. Place full file in data/reports/0031/
# Example: 0031-Contract Item Price Cat Pkg Extract 1108.xlsx

# 2. Run sync (auto-detects and processes full backup)
python main.py sync

# 3. Verify database reset
python main.py status
# Should show: last_full_backup updated, applied_incrementals reset to 0
```

### End-to-End Processing

```bash
# Process everything from sync to export
python main.py all

# Or step-by-step:
python main.py sync       # Phase 0
python main.py integrate  # Phase 0 + 1
python main.py classify   # Phase 0 + 1 + 2
python main.py export     # Phase 0 + 1 + 2 + 3
```

---

## Troubleshooting

### Check Logs

**Today's log:**
```bash
# Windows
type logs\pipeline_20251119.log

# Linux/Mac
cat logs/pipeline_20251119.log
```

**Search for errors:**
```bash
# Windows
findstr "ERROR" logs\pipeline_20251119.log

# Linux/Mac
grep "ERROR" logs/pipeline_20251119.log
```

### Review Reports

**Validation & Change Reports:**
- Location: `data/database/audit/`
- Files: `validation_and_changes_report_<date>.md` and `.xlsx`
- Check validation issues and change summaries

### Common Issues

**Issue: "No incremental files found"**
- Check file naming matches pattern in config
- Verify files are in `data/reports/0031/`
- Check if files were already processed (in archive)

**Issue: "Database not found"**
- Run `python main.py sync` first to create baseline
- Ensure full backup file is in `data/reports/0031/`

**Issue: "Validation warnings"**
- Review Excel report in `data/database/audit/`
- Check "Validation Issues" sheet for details
- Warnings don't stop processing - review and address separately

### Manual Recovery

**Restore from backup:**
```bash
# Copy latest backup to main database
# Windows
copy data\database\backup\0031_backup_20251119_*.parquet data\database\0031.parquet

# Linux/Mac
cp data/database/backup/0031_backup_20251119_*.parquet data/database/0031.parquet
```

**Re-process archived files:**
```bash
# Move files back from archive
# Windows
move data\reports\archive\*.xlsx data\reports\0031\

# Linux/Mac
mv data/reports/archive/*.xlsx data/reports/0031/

# Then run sync
python main.py sync
```

**Reset state (force full rebuild):**
```bash
# Delete state file
# Windows
del data\database\parquet_state.json

# Linux/Mac
rm data/database/parquet_state.json

# Run sync (will rebuild from all files)
python main.py sync
```

---

## Architecture

For detailed technical documentation, see:
- **[Phase0_Architecture.md](Phase0_Architecture.md)** - Complete Phase 0 technical documentation
  - Function call hierarchy
  - Module descriptions
  - Logger hierarchy
  - Design patterns
  - Data flow diagrams
  - Performance considerations

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     main.py (CLI)                       │
│           Logging, Config, Command Routing              │
└─────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┬──────────────────┐
        │                  │                  │                  │
        ▼                  ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Phase 0    │  │   Phase 1    │  │   Phase 2    │  │   Phase 3    │
│  Sync (DB)   │  │ Integration  │  │Classification│  │    Export    │
├──────────────┤  ├──────────────┤  ├──────────────┤  ├──────────────┤
│ database_    │  │ integrate.py │  │classification│  │  export.py   │
│  sync.py     │  │              │  │     .py      │  │              │
│      │       │  │              │  │              │  │              │
│      ▼       │  └──────────────┘  └──────────────┘  └──────────────┘
│ sync_core/   │
│ (internal)   │
└──────────────┘
```

### Directory Structure

```
cls_project/
├── main.py                      # CLI entry point
├── config/
│   └── config.json             # Configuration
├── logs/                        # Daily log files
├── src/
│   ├── logging_config.py       # Logging setup
│   ├── database_sync.py        # Phase 0 public API
│   ├── integrate.py            # Phase 1
│   ├── classification.py       # Phase 2
│   ├── export.py               # Phase 3
│   └── sync_core/              # Phase 0 internal modules
│       ├── orchestrator.py     # Main coordination
│       ├── processing.py       # Data processing
│       ├── quality.py          # Validation & changes
│       ├── reporting.py        # Report generation
│       ├── files.py            # File operations
│       ├── backup.py           # Backup management
│       └── state.py            # State tracking
└── data/
    ├── database/               # 0031.parquet + backups
    ├── reports/0031/           # Input: incremental files
    ├── reports/archive/        # Archived incrementals
    ├── daily_files/            # Input: Allscripts files
    ├── integrated/             # Phase 1 output
    ├── classified/             # Phase 2 output
    ├── exports/                # Phase 3 output
    └── ref_files/              # Reference files
```

---

## Important Notes

### Expected Row Count Discrepancies

The database may show fewer net new rows than the incremental file contains.
This is **expected** when:
- Historical duplicate records exist (same 5-key columns, different VPN)
- Incremental update provides the corrected version
- Process removes ALL duplicates and replaces with single correct record

**Example:**
- Incremental file: +100 rows
- Database duplicates cleaned: -15 rows  
- Net database change: +85 rows

**This is a feature, not a bug** - gradually cleaning data quality issues.

### Unique Key Definition

The pipeline uses a **5-column unique key** for all merging and deduplication:

1. PMM Item Number
2. Corp Acct
3. Vendor Code
4. Additional Cost Centre
5. Additional GL Account

Why 5 columns? Each level adds necessary uniqueness:
- PMM alone → multiple vendors
- PMM + Vendor → multiple corp accounts
- PMM + Vendor + Corp → multiple cost/GL variations
- All 5 together → truly unique

### Data Quality Validation

Three automatic checks run on every sync:

1. **Contract-Vendor Relationship** - One contract should have one vendor
2. **Blank Vendor Catalogue** - Flags unexpected blanks (with permitted list)
3. **Vendor Catalogue Consistency** - Same PMM+Vendor+Corp should have same catalogue

Validation warnings don't stop processing - review reports and address issues separately.

### Batch Processing Performance

**Single file mode:** ~15-20 seconds per file  
**Batch mode (5 files):** ~20-25 seconds total  
**Speedup:** 3-4x faster for multiple files

The pipeline automatically detects and batch-processes multiple files efficiently.

### Logging System

**Two levels of logging:**
- **Console (INFO):** High-level progress, user-friendly
- **Log file (DEBUG):** Detailed operations with module names

**Log files:**
- Daily files: `logs/pipeline_20251119.log`
- One file per day (all commands append)
- Retention: 30 days (configurable)

### State Management

The pipeline maintains state in `data/database/parquet_state.json`:
- Last update timestamp
- Applied incremental files (prevents reprocessing)
- Last full backup info
- Row/column counts
- Last validation summary

Delete this file to force a full rebuild from scratch.

---

## Contributing

For questions or issues, contact the development team.

## License

Internal use only - CLS Allscripts Data Processing Pipeline

---

**Last Updated:** 2025-11-19  
**Version:** 1.0  

**Python Version:** 3.1
