import logging
import sys
from datetime import date
from pathlib import Path

import polars as pl

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.constants import Columns0031
from src.sync.merge import filter_outdated_rows, prepare_merge_keys

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(message)s')
logger = logging.getLogger('test')


def test_smart_sync():
    print('=== Testing Smart Sync Logic ===')

    # 1. Setup Mock Data
    # Columns: PMM_ITEM_NUMBER, CORP_ACCT, VENDOR_CODE, ADD_COST_CENTRE, ADD_GL_ACCOUNT, ITEM_UPDATE_DATE

    # Current DB
    current_data = {
        Columns0031.PMM_ITEM_NUMBER: ['A', 'B', 'C', 'D'],
        Columns0031.CORP_ACCT: ['1', '1', '1', '1'],
        Columns0031.VENDOR_CODE: ['V1', 'V1', 'V1', 'V1'],
        Columns0031.ADD_COST_CENTRE: ['C1', 'C1', 'C1', 'C1'],
        Columns0031.ADD_GL_ACCOUNT: ['G1', 'G1', 'G1', 'G1'],
        Columns0031.ITEM_UPDATE_DATE: [
            date(2024, 1, 1),  # A: Baseline
            date(2024, 1, 1),  # B: Baseline
            None,  # C: Null date in DB
            date(2024, 1, 1),  # D: Baseline
        ],
    }
    current_df = pl.DataFrame(current_data)

    # Incremental Data
    incremental_data = {
        Columns0031.PMM_ITEM_NUMBER: ['A', 'B', 'C', 'D'],
        Columns0031.CORP_ACCT: ['1', '1', '1', '1'],
        Columns0031.VENDOR_CODE: ['V1', 'V1', 'V1', 'V1'],
        Columns0031.ADD_COST_CENTRE: ['C1', 'C1', 'C1', 'C1'],
        Columns0031.ADD_GL_ACCOUNT: ['G1', 'G1', 'G1', 'G1'],
        Columns0031.ITEM_UPDATE_DATE: [
            date(2023, 1, 1),  # A: OLDER -> Should be DROPPED
            date(2024, 2, 1),  # B: NEWER -> Should be KEPT
            date(2024, 1, 1),  # C: NEW vs NULL -> Should be KEPT
            date(2024, 1, 1),  # D: SAME -> Should be DROPPED (Strict Smart Sync)
        ],
    }
    incremental_df = pl.DataFrame(incremental_data)

    print('\nCurrent DB:')
    print(current_df)
    print('\nIncremental Input:')
    print(incremental_df)

    # 2. Run Filter
    filtered_df = filter_outdated_rows(current_df, incremental_df, logger)

    print('\nFiltered Result:')
    print(filtered_df)

    # 3. Verify Results
    result_items = filtered_df[Columns0031.PMM_ITEM_NUMBER].to_list()

    assert 'A' not in result_items, 'Item A (Older) should have been dropped'
    assert 'B' in result_items, 'Item B (Newer) should be kept'
    assert 'C' in result_items, 'Item C (New vs Null) should be kept'
    assert 'D' not in result_items, 'Item D (Same date) should be dropped (Strict Smart Sync)'

    print('\n✅ Verification SUCCESS!')


if __name__ == '__main__':
    test_smart_sync()
