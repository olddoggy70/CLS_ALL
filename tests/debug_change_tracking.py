import polars as pl
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('debug_tracking')


def original_track_changes(current_df, previous_df):
    """Original Python iteration method"""
    logger.info('Running ORIGINAL method...')

    # Create unique key column for joining (5-column key)
    current_with_key = current_df.with_columns(
        pl.concat_str(
            [
                pl.col('PMM Item Number').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Corp Acct').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Vendor Code').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional Cost Centre').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional GL Account').cast(pl.Utf8).fill_null('').str.strip_chars(),
            ],
            separator='|',
        ).alias('_unique_key')
    )

    previous_with_key = previous_df.with_columns(
        pl.concat_str(
            [
                pl.col('PMM Item Number').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Corp Acct').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Vendor Code').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional Cost Centre').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional GL Account').cast(pl.Utf8).fill_null('').str.strip_chars(),
            ],
            separator='|',
        ).alias('_unique_key')
    )

    current_keys = set(current_with_key.select('_unique_key').to_series().to_list())
    previous_keys = set(previous_with_key.select('_unique_key').to_series().to_list())
    updated_keys = current_keys & previous_keys

    updated_rows_current = current_with_key.filter(pl.col('_unique_key').is_in(list(updated_keys)))
    updated_rows_previous = previous_with_key.filter(pl.col('_unique_key').is_in(list(updated_keys)))

    changes_list = []

    if len(updated_rows_current) > 0:
        joined = updated_rows_current.join(updated_rows_previous, on='_unique_key', suffix='_previous')
        current_cols = set(current_with_key.columns) - {'_unique_key', 'source_file', '_merge_key'}

        for row in joined.iter_rows(named=True):
            for col in current_cols:
                prev_col = f'{col}_previous'
                if prev_col in row:
                    current_val = row[col]
                    previous_val = row[prev_col]

                    if current_val != previous_val:
                        changes_list.append(
                            {'Key': row['_unique_key'], 'Column': col, 'Old': str(previous_val), 'New': str(current_val)}
                        )
    return changes_list


def vectorized_track_changes(current_df, previous_df):
    """New Vectorized method"""
    logger.info('Running VECTORIZED method...')

    # Create unique key column for joining (5-column key)
    current_with_key = current_df.with_columns(
        pl.concat_str(
            [
                pl.col('PMM Item Number').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Corp Acct').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Vendor Code').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional Cost Centre').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional GL Account').cast(pl.Utf8).fill_null('').str.strip_chars(),
            ],
            separator='|',
        ).alias('_unique_key')
    )

    previous_with_key = previous_df.with_columns(
        pl.concat_str(
            [
                pl.col('PMM Item Number').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Corp Acct').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Vendor Code').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional Cost Centre').cast(pl.Utf8).fill_null('').str.strip_chars(),
                pl.col('Additional GL Account').cast(pl.Utf8).fill_null('').str.strip_chars(),
            ],
            separator='|',
        ).alias('_unique_key')
    )

    current_keys = set(current_with_key.select('_unique_key').to_series().to_list())
    previous_keys = set(previous_with_key.select('_unique_key').to_series().to_list())
    updated_keys = current_keys & previous_keys

    updated_rows_current = current_with_key.filter(pl.col('_unique_key').is_in(list(updated_keys)))
    updated_rows_previous = previous_with_key.filter(pl.col('_unique_key').is_in(list(updated_keys)))

    changes_list = []

    if len(updated_rows_current) > 0:
        compare_cols = list(set(current_with_key.columns) - {'_unique_key', 'source_file', '_merge_key'})

        curr_subset = updated_rows_current.select(['_unique_key', *compare_cols])
        prev_subset = updated_rows_previous.select(['_unique_key', *compare_cols])

        curr_long = curr_subset.melt(
            id_vars=['_unique_key'], value_vars=compare_cols, variable_name='Column', value_name='Current Value'
        )
        prev_long = prev_subset.melt(
            id_vars=['_unique_key'], value_vars=compare_cols, variable_name='Column', value_name='Previous Value'
        )

        joined_long = curr_long.join(prev_long, on=['_unique_key', 'Column'], how='inner')

        # CURRENT LOGIC
        changes_df_updates = joined_long.filter(
            pl.col('Current Value').cast(pl.Utf8).fill_null('').str.strip_chars()
            != pl.col('Previous Value').cast(pl.Utf8).fill_null('').str.strip_chars()
        )

        for row in changes_df_updates.iter_rows(named=True):
            changes_list.append(
                {
                    'Key': row['_unique_key'],
                    'Column': row['Column'],
                    'Old': str(row['Previous Value']),
                    'New': str(row['Current Value']),
                }
            )

    return changes_list


def main():
    # MOCK DATA
    # Case 1: Simple difference
    # Case 2: None vs Empty String (Original: Diff, Vectorized: Equal)
    # Case 3: Whitespace difference (Original: Diff, Vectorized: Equal)
    # Case 4: Type difference (Int vs Str)

    data_curr = {
        'PMM Item Number': ['1', '2', '3', '4'],
        'Corp Acct': ['A', 'A', 'A', 'A'],
        'Vendor Code': ['V', 'V', 'V', 'V'],
        'Additional Cost Centre': ['C', 'C', 'C', 'C'],
        'Additional GL Account': ['G', 'G', 'G', 'G'],
        'Description': ['New', '', ' Space ', '123'],
        'Price': [10.0, None, 5.0, 100],
    }

    data_prev = {
        'PMM Item Number': ['1', '2', '3', '4'],
        'Corp Acct': ['A', 'A', 'A', 'A'],
        'Vendor Code': ['V', 'V', 'V', 'V'],
        'Additional Cost Centre': ['C', 'C', 'C', 'C'],
        'Additional GL Account': ['G', 'G', 'G', 'G'],
        'Description': ['Old', None, 'Space', '123'],  # 2: None vs '', 3: 'Space' vs ' Space '
        'Price': [10.0, None, 5.0, '100'],  # 4: '100' vs 100
    }

    df_curr = pl.DataFrame(data_curr, strict=False)
    df_prev = pl.DataFrame(data_prev, strict=False)

    print('=== DATA PREVIEW ===')
    print('Current:')
    print(df_curr)
    print('Previous:')
    print(df_prev)

    orig_changes = original_track_changes(df_curr, df_prev)
    vect_changes = vectorized_track_changes(df_curr, df_prev)

    print(f'\nOriginal Changes Found: {len(orig_changes)}')
    for c in orig_changes:
        print(f"  {c['Key']} | {c['Column']}: '{c['Old']}' -> '{c['New']}'")

    print(f'\nVectorized Changes Found: {len(vect_changes)}')
    for c in vect_changes:
        print(f"  {c['Key']} | {c['Column']}: '{c['Old']}' -> '{c['New']}'")

    # Analyze discrepancies
    orig_keys = set((c['Key'], c['Column']) for c in orig_changes)
    vect_keys = set((c['Key'], c['Column']) for c in vect_changes)

    missing_in_vect = orig_keys - vect_keys
    if missing_in_vect:
        print('\n!!! MISSED BY VECTORIZED !!!')
        for k, col in missing_in_vect:
            # Find the original change detail
            detail = next(c for c in orig_changes if c['Key'] == k and c['Column'] == col)
            print(f"  {k} | {col}: '{detail['Old']}' -> '{detail['New']}'")


if __name__ == '__main__':
    main()
