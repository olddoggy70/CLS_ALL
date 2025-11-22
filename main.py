"""
Main application with multi-phase CLI

Phases:
  0. Sync        - Sync 0031 database from incremental backups
  1. Integration - Integrate daily files with 0031 baseline
  2. Classification - Classify records into buckets
  3. Export      - Generate export files

Note: All commands (except 'sync' and 'status') automatically run Phase 0
(database sync) first to ensure you're working with the latest data.
"""

import json
import os
import sys
import time
from pathlib import Path

try:
    import psutil
except ImportError:
    psutil = None

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.classify import get_classify_status, process_classify
from src.export import get_export_status, process_export
from src.integrate import get_integrate_status, process_integrate
from src.logging_config import setup_logging
from src.sync import process_sync


def load_config() -> dict:
    """Load configuration from JSON file"""
    base_dir = Path(__file__).parent
    config_file = base_dir / 'config' / 'config.json'

    if not config_file.exists():
        print(f'ERROR: Config file not found: {config_file}')
        print('Please create config/config.json before running this script.')
        sys.exit(1)

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            print(f'Configuration loaded from: {config_file}')
            return config
    except json.JSONDecodeError as e:
        print(f'ERROR: Invalid JSON in config file {config_file}')
        print(f'JSON Error: {e}')
        sys.exit(1)
    except Exception as e:
        print(f'ERROR: Failed to load config file {config_file}: {e}')
        sys.exit(1)


def get_config_paths(config: dict) -> dict:
    """Build paths dictionary from config"""
    base_dir = Path(__file__).parent
    db_folder = base_dir / config['paths']['database_folder']

    return {
        'base_dir': base_dir,
        # Parquet updater paths (Phase 0: Sync)
        'main_folder': base_dir / config['paths']['reports_folder'],
        'archive_folder': base_dir / config['paths']['reports_archive_folder'],
        'db_folder': db_folder,
        'db_file_path': db_folder / config['paths']['database_file'],
        'backup_folder': base_dir / config['paths']['backup_folder'],
        'audit_folder': base_dir / config['paths']['audit_folder'],
        'state_file': db_folder / config['update_settings']['state_file'],
        # Daily files (Phase 1: Integration)
        'daily_files_folder': base_dir / config['paths']['daily_files_folder'],
        'daily_archive_folder': base_dir / config['paths']['daily_archive_folder'],
        # Phase outputs
        'integrated_output': base_dir / config['paths']['integrated_output'],
        'classified_output': base_dir / config['paths']['classified_output'],
        'exports_output': base_dir / config['paths']['exports_output'],
        # Reference files
        'blank_vpn_permitted_file': base_dir / config['paths']['reference_files']['blank_vpn_permitted_file'],
        'mfn_mapping_file': base_dir / config['paths']['reference_files']['mfn_mapping'],
        'vn_mapping_file': base_dir / config['paths']['reference_files']['vn_mapping'],
        # Legacy paths (for compatibility)
        'csv_file': base_dir / config['paths'].get('test_csv', 'test.csv'),
        'export_dir': base_dir / config['paths'].get('export_folder', 'data/output'),
        # Logging
        'log_folder': base_dir / config['logging']['log_folder'],
    }


def run_sync_phase(config: dict, paths: dict, logger) -> bool:
    """Run Phase 0: Database Sync"""
    if not config.get('phases', {}).get('sync', {}).get('enabled', True):
        logger.info('Phase 0 (Sync): Skipped (disabled in config)')
        return True

    try:
        process_sync(config, paths, None)
        return True
    except Exception as e:
        logger.error(f'Phase 0 (Sync) failed: {e}')
        return False


def run_integrate_phase(config: dict, paths: dict, logger) -> bool:
    """Run Phase 1: Integration (with auto-sync)"""
    logger.info('=' * 60)
    logger.info('RUNNING: SYNC + INTEGRATE')
    logger.info('=' * 60)

    # Always run Phase 0 first
    if not run_sync_phase(config, paths, logger):
        logger.error('Phase 0 (Sync) failed, stopping')
        return False

    # Run Phase 1
    if not config.get('phases', {}).get('integration', {}).get('enabled', True):
        logger.info('Phase 1 (Integration): Skipped (disabled in config)')
        return True

    try:
        logger.info('')
        logger.info('=' * 60)
        logger.info('PHASE 1: INTEGRATION')
        logger.info('=' * 60)
        # success = process_integrate(config, paths, logger)
        success = process_integrate(config, paths, None)
        if not success:
            logger.error('Phase 1 (Integration) failed')
            return False
        return True
    except Exception as e:
        logger.error(f'Phase 1 (Integration) failed: {e}')
        return False


def run_classify_phase(config: dict, paths: dict, logger) -> bool:
    """Run Phases 0-2: Sync + Integration + Classification"""
    logger.info('=' * 60)
    logger.info('RUNNING: SYNC + INTEGRATE + CLASSIFY')
    logger.info('=' * 60)

    # Run Phase 0 + 1
    if not run_integrate_phase(config, paths, logger):
        return False

    # Run Phase 2
    if not config.get('phases', {}).get('classification', {}).get('enabled', True):
        logger.info('Phase 2 (Classification): Skipped (disabled in config)')
        return True

    try:
        logger.info('')
        logger.info('=' * 60)
        logger.info('PHASE 2: CLASSIFICATION')
        logger.info('=' * 60)
        success = process_classify(config, paths, logger)
        if not success:
            logger.error('Phase 2 (Classification) failed')
            return False
        return True
    except Exception as e:
        logger.error(f'Phase 2 (Classification) failed: {e}')
        return False


def run_export_phase(config: dict, paths: dict, logger) -> bool:
    """Run Full Pipeline: Phases 0-3"""
    logger.info('=' * 60)
    logger.info('RUNNING: FULL PIPELINE (SYNC + INTEGRATE + CLASSIFY + EXPORT)')
    logger.info('=' * 60)

    # Run Phase 0 + 1 + 2
    if not run_classify_phase(config, paths, logger):
        return False

    # Run Phase 3
    if not config.get('phases', {}).get('export', {}).get('enabled', True):
        logger.info('Phase 3 (Export): Skipped (disabled in config)')
        return True

    try:
        logger.info('')
        logger.info('=' * 60)
        logger.info('PHASE 3: EXPORT')
        logger.info('=' * 60)
        success = process_export(config, paths, logger)
        if not success:
            logger.error('Phase 3 (Export) failed')
            return False
        return True
    except Exception as e:
        logger.error(f'Phase 3 (Export) failed: {e}')
        return False


def show_status(config: dict, paths: dict, logger):
    """Show status of all phases"""
    logger.info('=' * 60)
    logger.info('PROCESSING PIPELINE STATUS')
    logger.info('=' * 60)

    # Phase 0: Sync status
    logger.info('')
    logger.info('--- Phase 0: Database Sync ---')
    db_file = paths['db_file_path']
    if db_file.exists():
        from datetime import datetime

        mod_time = datetime.fromtimestamp(db_file.stat().st_mtime)
        file_size_mb = db_file.stat().st_size / (1024 * 1024)
        logger.info(f'Status: ✓ Up to date')
        logger.info(f'Database: {db_file.name}')
        logger.info(f'Size: {file_size_mb:.2f} MB')
        logger.info(f'Last updated: {mod_time.strftime("%Y-%m-%d %H:%M:%S")}')
    else:
        logger.info('Status: ⚠ Database not found')

    # Phase 1: Integration status
    logger.info('')
    logger.info('--- Phase 1: Integration ---')
    status1 = get_integrate_status(config, paths)
    logger.info(f'Status: {status1["status"]}')
    logger.info(f'Output files: {status1["output_files"]}')
    if status1.get('latest_output'):
        logger.info(f'Latest: {status1["latest_output"]}')

    # Phase 2: Classification status
    logger.info('')
    logger.info('--- Phase 2: Classification ---')
    status2 = get_classify_status(config, paths)
    logger.info(f'Status: {status2["status"]}')
    logger.info(f'Buckets: {status2.get("buckets", 0)}')
    logger.info(f'Total files: {status2.get("total_files", 0)}')

    # Phase 3: Export status
    logger.info('')
    logger.info('--- Phase 3: Export ---')
    status3 = get_export_status(config, paths)
    logger.info(f'Status: {status3["status"]}')
    logger.info(f'Export files: {status3.get("export_files", 0)}')
    if status3.get('latest_export'):
        logger.info(f'Latest: {status3["latest_export"]}')

    logger.info('')
    logger.info('=' * 60)


def print_usage():
    """Print usage information"""
    print('\nData Processing Pipeline CLI')
    print('=' * 60)
    print('\nUsage:')
    print('  python main.py sync        Update 0031 database only')
    print('  python main.py integrate   Sync database + integrate daily files')
    print('  python main.py classify    Full pipeline up to classification')
    print('  python main.py export      Full pipeline including export')
    print('  python main.py all         (same as export - full pipeline)')
    print('  python main.py status      Show current status')
    print('  python main.py --help      Show this help')
    print('\nPhases:')
    print('  0. Sync          Sync 0031 database from incremental backups')
    print('  1. Integration   Integrate daily files with 0031 baseline')
    print('  2. Classification Classify records into buckets')
    print('  3. Export        Generate export files')
    print('\nNote:')
    print('  All commands (except sync and status) automatically run Phase 0')
    print("  (database sync) first to ensure you're working with the latest data.")
    print('\nExamples:')
    print('  python main.py integrate   # Most common: sync DB + process daily files')
    print('  python main.py export      # Complete end-to-end processing')
    print('  python main.py sync        # Only update database, no further processing')
    print('  python main.py status      # Check current pipeline status')
    print()


def main():
    """Main CLI entry point"""

    # Set process to high priority for performance
    if psutil:
        try:
            p = psutil.Process(os.getpid())
            p.nice(psutil.HIGH_PRIORITY_CLASS)
        except Exception:
            pass  # Ignore if can't set priority (e.g., on Linux/Mac)

    # Handle help flag
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h', 'help']:
        print_usage()
        sys.exit(0)

    # Load config FIRST (needed for log folder path)
    config = load_config()
    paths = get_config_paths(config)

    # Setup logging
    console_level = config.get('logging', {}).get('console_level', 'INFO')
    file_level = config.get('logging', {}).get('file_level', 'DEBUG')
    enable_timing = config.get('logging', {}).get('enable_timing', False)
    logger, log_file = setup_logging(paths['log_folder'], console_level, file_level, enable_timing)

    logger.info('=== Data Processing Pipeline ===')
    logger.info(f'Log file: {log_file.name}')

    if len(sys.argv) < 2:
        print_usage()
        sys.exit(0)

    command = sys.argv[1].lower()

    logger.info(f'Command: {command}')
    logger.info(f'Time: {time.strftime("%Y-%m-%d %H:%M:%S")}')

    try:
        if command in ['all', 'export']:
            # Full pipeline (Phases 0-3)
            success = run_export_phase(config, paths, logger)
            if success:
                logger.info('')
                logger.info('=== Full Pipeline Completed Successfully ===')
            else:
                logger.error('Pipeline failed')
                sys.exit(1)

        elif command == 'sync':
            # Phase 0 only
            logger.info('=' * 60)
            logger.info('PHASE 0: DATABASE SYNC')
            logger.info('=' * 60)
            process_sync(config, paths, logger)

        elif command == 'integrate':
            # Phases 0-1
            success = run_integrate_phase(config, paths, logger)
            if success:
                logger.info('')
                logger.info('=== Sync + Integration Completed Successfully ===')
            else:
                logger.error('Integration pipeline failed')
                sys.exit(1)

        elif command == 'classify':
            # Phases 0-2
            success = run_classify_phase(config, paths, logger)
            if success:
                logger.info('')
                logger.info('=== Classification Pipeline Completed Successfully ===')
            else:
                logger.error('Classification pipeline failed')
                sys.exit(1)

        elif command == 'status':
            show_status(config, paths, logger)

        else:
            logger.error(f'Unknown command: {command}')
            print_usage()
            sys.exit(1)

        logger.info('')
        logger.info('=== Pipeline Command Completed ===')

    except KeyboardInterrupt:
        logger.warning('Process interrupted by user')
        sys.exit(130)

    except Exception as e:
        logger.error(f'Error: {e}')
        import traceback

        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
