
# CLS Allscripts Data Processing Pipeline

![Python](https://img.shields.io/badge/python-3.12-blue)
![Version](https://img.shields.io/badge/version-1.0.0-green)
![Status](https://img.shields.io/badge/status-stable-success)
![License](https://img.shields.io/badge/license-internal-lightgrey)

A four-phase pipeline for processing contract pricing data from BC to Allscripts: database synchronization, daily integration with enrichment and validation, classification (in development), and export (in development).

## Table of Contents

- [Overview](#overview)
- [Quick Reference](#quick-reference)
- [What's New in Version 1.0.0](#whats-new-in-version-100)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Pipeline Phases](#pipeline-phases)
- [Configuration](#configuration)
- [Visual Examples](#visual-examples)
- [Configuration Examples](#configuration-examples)
- [Common Workflows](#common-workflows)
- [Troubleshooting](#troubleshooting)
- [Architecture](#architecture)
- [Important Notes](#important-notes)
- [FAQ](#faq)
- [Common Error Codes](#common-error-codes)



---

## Overview

This pipeline automates the processing of healthcare procurement data through four phases:

1. **Phase 0 (Sync)** - Synchronize the 0031 database from incremental backups
2. **Phase 1 (Integration)** - Integrate daily Allscripts files with the 0031 baseline
3. **Phase 2 (Classification)** - Classify records into processing buckets
4. **Phase 3 (Export)** - Generate final export files

The pipeline handles ~1.4M records with automatic change tracking, data validation, and duplicate detection.

## Quick Reference

| Command | What It Does | When To Use |
|---------|--------------|-------------|
| `python main.py status` | Show current pipeline status | Check what's been processed |
| `python main.py sync` | Update 0031 database only | Process new 0031 files |
| `python main.py integrate` | Sync + integrate daily files | Daily morning routine |
| `python main.py export` | Full pipeline to final export | Generate deliverables |
| `python main.py all` | Same as export | Complete end-to-end run |

**Most Common Workflow:** `python main.py integrate` (runs Phase 0 → 1)

---

## What's New in Version 1.0.0

**First Stable Release** - Phase 0 (Database Sync) and Phase 1 (Integration) are production-ready!

### Major Features
- ✨ **Smart Sync** - Prevents data overwrites by filtering outdated rows (Incoming Date > Existing Date)
- ⚡ **Optimized Change Tracking** - 10x faster using vectorized Polars operations
- 📊 **Improved Reporting** - New rows now in clean wide format; field-level tracking for updates only
- 🔧 **Fixed Negative Skipped Rows** - Added deduplication logic to prevent join explosion
- 🎯 **Dynamic Integration Status** - Correctly detects output format from config.json

### Bug Fixes
- Fixed `get_integrate_status` to check for configured output format instead of hardcoded `.parquet`
- Fixed file sorting to reliably identify "Latest" integrated file by modification time
- Corrected deduplication in `filter_outdated_rows` to prevent inflated skipped row counts

### Architecture Updates
- Reorganized into package structure: `src/sync/`, `src/integrate/`, `src/classify/`, `src/export/`
- Added `merge.py` for Smart Sync logic
- Split transformation and quality tracking into separate modules

---

## Features

### Phase 0: Database Sync
- ✅ **Automatic detection** of full backups and incrementals
- ✅ **Batch processing** - processes multiple files efficiently in one operation
- ✅ **Smart Sync** - strictly filters out outdated rows (Incoming > Existing)
- ✅ **Change tracking** - per-file change analysis with field-level detail
- ✅ **Duplicate detection** - identifies items updated across multiple files
- ✅ **Data validation** - contract-vendor relationships, blank catalogues, consistency checks
- ✅ **Automatic archiving** - processed files moved to archive with configurable retention
- ✅ **Backup & recovery** - automatic backups before every modification

### Phase 1: Integration
- ✅ **Enrichment** - adds PMM mappings, vendor information, contract analysis
- ✅ **Reference mappings** - MFN and VN mappings from reference files
- ✅ **Duplicate handling** - collapses multiple PMM candidates per record
- ✅ **Excel generation** - formatted exports with date ranges
- ✅ **Automatic dating** - filename includes data date range

### Phase 2: Classification
- 🚧 **In development** - classify records into update/create/link buckets

### Phase 3: Export
- 🚧 **In development** - generate final CSV export files for system upload, Excel for reference

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
     - `MFN Mapping.xlsx` - Manufacturer number mappings
     - `VN Mapping.xlsx` - Vendor number mappings
     - `Blank Vpn Permitted.xlsx` - Approved list of items allowed to have blank vendor catalogues

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
   This creates the baseline `0031.parquet` database (~100MB, 1.4M rows).

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
| `classify` | Full pipeline up to classification | Phase 0 → 1 → 2* |
| `export` | Complete end-to-end processing | Phase 0 → 1 → 2* → 3* |
| `all` | Same as export (full pipeline) | Phase 0 → 1 → 2* → 3* |
| `status` | Show current pipeline status | Status only |
| `--help` | Show help information | Help only |

\* Phase 2-3 currently in development

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
1. **Auto-Detection:** Identifies file type (full vs incremental)
2. **Backup:** Creates backup before modifications
3. **Smart Sync:** Filters out outdated rows (Incoming Date > Existing Date)
   - Example: If DB has Item A with date 2025-11-20, incoming Item A with date 2025-11-19 is rejected
   - Prevents accidental overwrites with stale data
   - See `src/sync/merge.py::filter_outdated_rows()` for implementation
4. **Change Tracking:** Applies changes with field-level tracking
   - **New Rows:** Reported in full wide format (no field-level melt)
   - **Updated Rows:** Detailed field-level comparison (vectorized for speed)
   - **Whitespace Tracking:** Tracks whitespace changes (e.g., 'ABC' vs 'AB C')
5. **Validation:** Checks for data quality issues
6. **Reporting:** Generates comprehensive Excel/Markdown reports
7. **Archiving:** Archives processed files

**Outputs:**
- `data/database/0031.parquet` - Main database
- `data/database/audit/` - Reports (`validation_and_changes_report_<date>.xlsx`)
  - **Summary:** High-level metrics (New, Updated, Skipped)
  - **Per-File Summary:** Original Rows, Dropped Rows, Accepted Rows
  - **New Rows:** Full records of new items
  - **Updated Rows:** Field-level changes (Old vs New Value)
  - **Accepted Rows by Date:** Breakdown of valid data by date
- `data/reports/archive/` - Archived files

**Key Features:**
- **Vectorized Comparison:** 10x faster change tracking using Polars
- **Strict Smart Sync:** Prevents overwriting newer data with older/same-date data
- **Clean Reporting:** "New Rows" are easy to read; "Updated Rows" focus on real changes
- **5-column unique key:** `PMM Item Number + Corp Acct + Vendor Code + Additional Cost Centre + Additional GL Account`

### Phase 1: Integration

**Purpose:** Integrate daily Allscripts files with the 0031 baseline database.

**Input Files:**
- `data/daily_files/Allscripts_historical_deduped_*.xlsx`

**Processing:**
1. **Source Tracking:** Reads daily files with source tracking
2. **Enrichment:** Enriches with 0031 database lookups (PMM mappings, vendor info)
3. **Reference Mapping:** Adds contract analysis and reference mappings
4. **Quality Flagging:** Flags duplicates and inconsistencies

**Outputs:**
- `data/output/integrated/integrated_<timestamp>.<format>` 
  - Format is configurable in `config.json` → `integration.output_format` (options: `xlsx`, `parquet`, `csv`)

**Key Features:**
- **Dynamic Enrichment:** Combines 0031 database with daily Allscripts data
- **Flexible Output:** Supports Excel, Parquet, or CSV formats
- **Date Range Tracking:** Automatic filename dating based on data date range
- **Duplicate Detection:** Identifies and flags duplicate records

### Phase 2: Classification

**Purpose:** Classify records into processing buckets.

**Status:** 🚧 In development

**Planned Buckets:**
- Update - Records to update
- Create - New records to create
- Vendor Link - Vendor linkage updates
- Contract Link - Contract linkage updates

### Phase 3: Export

**Purpose:** Generate final Excel and CSV exports from processed data.

**Status:** 🚧 In development

**Planned Output:**
- `data/output/exports/ExportFile_<date_range>.xlsx` (reference)
- `data/output/exports/ExportFile_<date_range>.csv` (system upload)
- Example: `ExportFile_2025-11-01~2025-11-06.xlsx` and `ExportFile_2025-11-01~2025-11-06.csv`

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

## Visual Examples

### Audit Report Structure

The pipeline generates comprehensive Excel reports in `data/database/audit/`. Here's what each sheet contains:

**Sheet 1: Summary**
```
┌─────────────────────────────┬────────┐
│ Metric                      │ Count  │
├─────────────────────────────┼────────┤
│ Total Files Processed       │      5 │
│ Total Original Rows         │ 68,432 │
│ Total Dropped Rows          │  1,234 │
│ Total Accepted Rows         │ 67,198 │
│ New Rows                    │  5,432 │
│ Updated Rows                │  1,876 │
│ Skipped Rows (Outdated)     │     89 │
│ Unchanged Rows              │ 59,801 │
└─────────────────────────────┴────────┘
```

**Sheet 2: Per-File Summary**
```
┌────────────────────────────────┬──────────┬─────────┬──────────┬──────────┐
│ File Name                      │ Original │ Dropped │ Accepted │ Date     │
├────────────────────────────────┼──────────┼─────────┼──────────┼──────────┤
│ 0031_Extract_2025_11_19.xlsx   │   13,245 │     234 │   13,011 │ 11/19/25 │
│ 0031_Extract_2025_11_20.xlsx   │   14,123 │     456 │   13,667 │ 11/20/25 │
│ 0031_Extract_2025_11_21.xlsx   │   12,987 │     198 │   12,789 │ 11/21/25 │
└────────────────────────────────┴──────────┴─────────┴──────────┴──────────┘
```

**Sheet 3: New Rows** (Full wide format)
```
┌──────────┬───────────┬───────────┬──────────────┬─────────────┬─────┐
│ PMM Item │ Corp Acct │ Vendor    │ Cost Centre  │ GL Account  │ ... │
├──────────┼───────────┼───────────┼──────────────┼─────────────┼─────┤
│ PMM12345 │ CORP001   │ VEN0123   │ CC001        │ GL1234      │ ... │
│ PMM12346 │ CORP002   │ VEN0124   │ CC002        │ GL1235      │ ... │
└──────────┴───────────┴───────────┴──────────────┴─────────────┴─────┘
```

**Sheet 4: Updated Rows** (Field-level changes)
```
┌──────────┬────────────┬───────────┬───────────┬──────────┐
│ PMM Item │ Field Name │ Old Value │ New Value │ Date     │
├──────────┼────────────┼───────────┼───────────┼──────────┤
│ PMM12345 │ Unit Price │    10.50  │    11.25  │ 11/20/25 │
│ PMM12345 │ Vendor     │  VEN0100  │  VEN0101  │ 11/20/25 │
└──────────┴────────────┴───────────┴───────────┴──────────┘
```

**Sheet 5: Accepted Rows by Date**
```
┌─────────────────┬──────────┐
│ Item Date       │ Count    │
├─────────────────┼──────────┤
│ 2025-11-19      │   13,011 │
│ 2025-11-20      │   13,667 │
│ 2025-11-21      │   12,789 │
└─────────────────┴──────────┘
```

### Smart Sync in Action

**Before Smart Sync (Problem):**
```
Database has:
  Item A, Date: 2025-11-20, Price: $15.00

Incoming file has:
  Item A, Date: 2025-11-19, Price: $12.00  ← Older data!

Without Smart Sync → Database gets overwritten with old price ($12.00) ❌
```

**After Smart Sync (Solution):**
```
Database has:
  Item A, Date: 2025-11-20, Price: $15.00

Incoming file has:
  Item A, Date: 2025-11-19, Price: $12.00

Smart Sync → Rejects row (2025-11-19 < 2025-11-20)
           → Database keeps $15.00 ✅
           → Audit report shows: "Skipped Rows (Outdated): 1"
```

### State File Example

`data/database/parquet_state.json`:
```json
{
  "last_update": "2025-11-21T08:45:23",
  "database_path": "data/database/0031.parquet",
  "last_full_backup": {
    "file": "0031-Contract Item Price Cat Pkg Extract 1108.xlsx",
    "processed_date": "2025-11-08T14:20:15"
  },
  "applied_incrementals": [
    "0031-Contract Item Price Cat Pkg Extract 2025_11_19.xlsx",
    "0031-Contract Item Price Cat Pkg Extract 2025_11_20.xlsx",
    "0031-Contract Item Price Cat Pkg Extract 2025_11_21.xlsx"
  ],
  "row_count": 1402345,
  "column_count": 47
}
```

---

## Configuration Examples

### Development Environment

**config.json for local testing:**
```json
{
  "file_patterns": {
    "0031_incremental": {
      "pattern": "0031-Contract Item Price Cat Pkg Extract *.xlsx",
      "directory": "data/reports/0031",
      "archive_after_processing": false
    },
    "0031_full": {
      "pattern": "0031-Contract Item Price Cat Pkg Extract [0-9][0-9][0-9][0-9].xlsx",
      "directory": "data/reports/0031",
      "keep_only_latest": false
    }
  },
  "processing_schedule": {
    "process_incrementals_on_startup": true,
    "auto_detect_weekly_full": true,
    "max_incrementals_per_run": 3
  },
  "integration": {
    "output_format": "xlsx",
    "daily_files_path": "data/daily_files",
    "output_path": "data/output/integrated"
  },
  "logging": {
    "console_level": "DEBUG",
    "file_level": "DEBUG",
    "log_folder": "logs",
    "retention_days": 7
  },
  "update_settings": {
    "backup_before_update": true,
    "backup_retention_days": 3
  }
}
```

### Production Environment

**config.json for production:**
```json
{
  "file_patterns": {
    "0031_incremental": {
      "pattern": "0031-Contract Item Price Cat Pkg Extract *.xlsx",
      "directory": "D:/Production/Data/Reports/0031",
      "archive_after_processing": true
    },
    "0031_full": {
      "pattern": "0031-Contract Item Price Cat Pkg Extract [0-9][0-9][0-9][0-9].xlsx",
      "directory": "D:/Production/Data/Reports/0031",
      "keep_only_latest": true
    }
  },
  "processing_schedule": {
    "process_incrementals_on_startup": true,
    "auto_detect_weekly_full": true,
    "max_incrementals_per_run": 10
  },
  "integration": {
    "output_format": "parquet",
    "daily_files_path": "D:/Production/Data/DailyFiles",
    "output_path": "D:/Production/Data/Output/Integrated"
  },
  "archive_settings": {
    "enabled": true,
    "retention_days": 90,
    "archive_path": "D:/Production/Data/Archive"
  },
  "logging": {
    "console_level": "INFO",
    "file_level": "DEBUG",
    "log_folder": "D:/Production/Logs",
    "retention_days": 30
  },
  "update_settings": {
    "backup_before_update": true,
    "backup_retention_days": 14
  }
}
```

### Typical Audit Report Output

**Example from a real run:**
```
=== Validation and Changes Report ===
Date: 2025-11-21 08:45:23

SUMMARY
-------
Files Processed: 5
Total Original Rows: 68,432
Total Dropped Rows: 1,234 (duplicate keys, invalid dates)
Total Accepted Rows: 67,198
Database Rows After Merge: 1,402,345

CHANGE ANALYSIS
---------------
New Rows: 5,432 (items not in database)
Updated Rows: 1,876 (existing items with changes)
  - Price changes: 1,234
  - Vendor changes: 342
  - Other field updates: 300
Skipped Rows (Outdated): 89 (Smart Sync rejected)
Unchanged Rows: 59,801 (no changes detected)

VALIDATION WARNINGS
------------------
Contract-Vendor Mismatches: 12 items
  - Contract C001 has 2 different vendors
  - Contract C045 has 3 different vendors

Blank Vendor Catalogues: 45 items
  - 15 are in permitted list (OK)
  - 30 require review

Vendor Catalogue Inconsistencies: 8 items
  - Same PMM+Vendor+Corp with different catalogues

PERFORMANCE
-----------
Processing Time: 23.4 seconds
Files/Second: 0.21
Rows/Second: 2,873
Memory Peak: 487 MB
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

**Fix corrupted state file:**
```bash
# If you see "JSON decode error" or "Invalid state file"
# Windows
del data\database\parquet_state.json

# Linux/Mac
rm data/database/parquet_state.json

# Run status to regenerate
python main.py status
```

**Handle duplicate key errors:**
```bash
# If you see "Duplicate key detected" in logs
# This is handled automatically by Smart Sync, but to verify:
python main.py sync
# Check the audit report for "Skipped Rows (Outdated)"
```

**Recover from failed integration:**
```bash
# Delete partial integrated file
# Windows
del data\\output\\integrated\\integrated_*.xlsx

# Linux/Mac
rm data/output/integrated/integrated_*.xlsx

# Re-run integration
python main.py integrate
```

---

## Architecture

For detailed technical documentation, see:
- **[Phase0&1_Architecture.md](Phase0&1_Architecture.md)** - Complete Phase 0 & 1 technical documentation
  - Function call hierarchy and module interactions
  - Detailed descriptions of each module (orchestrator, core, merge, quality, reporting)
  - Logger hierarchy and configuration
  - Design patterns and best practices
  - Data flow diagrams with technical depth
  - Performance considerations and optimization strategies

### Data Flow Overview

```
┌─────────────────────┐
│  Source Files       │
│  - Full Backups     │──┐
│  - Incrementals     │  │
│  - Daily Allscripts │  │
└─────────────────────┘  │
                         ▼
                  ┌──────────────┐
                  │   Phase 0    │
                  │  Database    │──▶ 0031.parquet (1.4M rows)
                  │    Sync      │──▶ Audit Reports
                  └──────────────┘──▶ Archived Files
                         │
                         ▼
                  ┌──────────────┐
                  │   Phase 1    │
                  │ Integration  │──▶ data/output/integrated/
                  └──────────────┘
                         │
                         ▼
                  ┌──────────────┐
                  │   Phase 2    │
                  │Classification│──▶ data/output/classified/
                  └──────────────┘──▶ (In Development)
                         │
                         ▼
                  ┌──────────────┐
                  │   Phase 3    │
                  │    Export    │──▶ data/output/exports/ (.xlsx/.csv)
                  └──────────────┘──▶ (In Development)
```

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
│ sync/        │  │ integrate/   │  │  classify/   │  │   export/    │
│ (package)    │  │ (package)    │  │  (package)   │  │  (package)   │
│      │       │  │      │       │  │              │  │              │
│      ▼       │  │      ▼       │  └──────────────┘  └──────────────┘
│ pipeline.py  │  │ pipeline.py  │
│      │       │  │      │       │
│      ▼       │  │      ▼       │
│ orchestrator │  │ ingest.py    │
│ core.py      │  │ baseline.py  │
│ merge.py     │  │ enrichment.py│
│ quality.py   │  │              │
└──────────────┘  └──────────────┘
```

### Directory Structure

```
cls_project/
├── main.py                      # CLI entry point
├── config/
│   └── config.json             # Configuration
├── logs/                        # Daily log files
├── src/
│   ├── __init__.py
│   ├── logging_config.py       # Logging setup
│   ├── constants.py            # Shared constants
│   ├── sync/                   # Phase 0: Database Sync
│   │   ├── pipeline.py         # Public API
│   │   ├── orchestrator.py     # Main coordination
│   │   ├── core.py             # Core sync logic
│   │   ├── merge.py            # Smart Sync & Deduplication
│   │   ├── quality.py          # Change Tracking & Validation
│   │   ├── reporting.py        # Report generation
│   │   ├── ingest.py           # File reading
│   │   ├── transformation.py   # Data transformation
│   │   ├── file_discovery.py   # File pattern matching
│   │   ├── backup.py           # Backup management
│   │   └── sync_state.py       # State tracking
│   ├── integrate/              # Phase 1: Integration
│   │   ├── pipeline.py         # Main logic
│   │   ├── ingest.py           # Data ingestion
│   │   ├── baseline.py         # Database prep
│   │   └── enrichment.py       # Data enrichment
│   ├── classify/               # Phase 2: Classification
│   │   └── ...                 # (In development)
│   ├── export/                 # Phase 3: Export
│   │   └── ...                 # (In development)
│   └── utils/                  # Shared utilities
│       └── ...
└── data/
    ├── database/               # 0031.parquet + backups
    ├── reports/0031/           # Input: incremental files
    ├── reports/archive/        # Archived incrementals
    ├── daily_files/            # Input: Allscripts files
    ├── output/                 # Phase outputs
    │   ├── integrated/         # Phase 1 output
    │   ├── classified/         # Phase 2 output
    │   └── exports/            # Phase 3 output
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

**Context (typical workload):**
- Each incremental file: ~5,000-15,000 rows
- 5 files combined: ~50,000-75,000 rows processed
- File sizes: 3-8 MB per Excel file
- Database size after merge: ~50 MB (1.4M rows)

**Test Environment:** 16GB RAM, SSD storage

The pipeline automatically detects and batch-processes multiple files efficiently using:
- Polars lazy evaluation for memory efficiency
- Vectorized operations for comparison logic
- Single write operation at the end

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

---

## FAQ

### General Questions

**Q: How often should I run the sync?**  
A: Run `python main.py integrate` daily to process overnight incremental files. Full backups are processed automatically when detected.

**Q: What happens if I run sync twice with the same file?**  
A: The pipeline tracks processed files in `parquet_state.json` and will skip already-processed incrementals automatically.

**Q: Can I process files out of order?**  
A: Yes, Smart Sync handles this. Files are processed by date, not by the order you run them. Older data won't overwrite newer data.

**Q: How do I know if Smart Sync rejected rows?**  
A: Check the "Skipped Rows (Outdated)" metric in the audit report (`data/database/audit/validation_and_changes_report_<date>.xlsx`).

**Q: Why are my "New Rows" counts different from the file row count?**  
A: Smart Sync may reject outdated rows, and deduplication removes duplicates. This is expected and documented in the audit report.

### Technical Questions

**Q: What file formats are supported for integration output?**  
A: Configurable in `config.json` → `integration.output_format`. Supports: `xlsx`, `parquet`, `csv`. Default is `parquet`.

**Q: How much disk space does the pipeline need?**  
A: Minimum 500 MB. Breakdown: 50 MB database, 100 MB logs/backups, 200+ MB for integrated files (depends on daily volume).

**Q: Can I run multiple phases in parallel?**  
A: No. Phases must run sequentially (Phase 0 → 1 → 2 → 3). Use `python main.py all` for automatic sequencing.

**Q: What happens to validation warnings?**  
A: Warnings don't stop processing. Review the "Validation Issues" sheet in audit reports and address separately.

---

## Common Error Codes

### Phase 0 Errors

**`FileNotFoundError: 0031.parquet not found`**
- **Cause:** Database file doesn't exist
- **Solution:** Run `python main.py sync` with a full backup file to create the baseline

**`SchemaError: type String is incompatible with Float32`**
- **Cause:** Column type mismatch between database and incoming file
- **Solution:** Check `src/sync/transformation.py` for schema enforcement. File may have incorrect data types.

**`ValueError: No files matching pattern found`**
- **Cause:** No incremental files in `data/reports/0031/`
- **Solution:** Verify files exist and match the pattern in `config.json` → `file_patterns.0031_incremental.pattern`

**`JSONDecodeError: Invalid state file`**
- **Cause:** Corrupted `parquet_state.json`
- **Solution:** Delete the file and run `python main.py status` to regenerate

### Phase 1 Errors

**`KeyError: 'PMM Item Number'`**
- **Cause:** Required column missing from daily file
- **Solution:** Verify Allscripts file has all required columns (see `src/constants.py` for list)

**`MemoryError: Unable to allocate array`**
- **Cause:** Insufficient RAM for large file processing
- **Solution:** Increase system RAM or process files in smaller batches

**`PolarsError: unable to open file`**
- **Cause:** File is locked or corrupted
- **Solution:** Close Excel if file is open, or verify file integrity

### General Errors

**`PermissionError: [WinError 32] The process cannot access the file`**
- **Cause:** File is opened in Excel or another program
- **Solution:** Close the file in Excel/other programs before running the pipeline

**`PermissionError: [Errno 13] Permission denied`**
- **Cause:** Insufficient file system permissions (Linux/Mac)
- **Solution:** Ensure write permissions on data directories

**`ModuleNotFoundError: No module named 'polars'`**
- **Cause:** Missing dependencies
- **Solution:** Run `pip install -r requirements.txt`

---

## Contributing

For questions or issues, contact the development team.

## License

Internal use only - CLS Allscripts Data Processing Pipeline

---

**Last Updated:** 2025-11-25 18:25  
**Version:** 1.0.0  
**Python Version:** 3.12

