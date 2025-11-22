import logging

import polars as pl
import pytest
from src.utils.date_utils import extract_date_range

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)

class TestExtractDateRange:
    def test_single_date(self):
        """Test extraction of a single date."""
        df = pl.DataFrame({
            "Date": ["2023-01-01", "2023-01-01"]
        })
        result = extract_date_range(df, "Date", "%Y-%m-%d")
        assert result == "2023-01-01"

    def test_date_range(self):
        """Test extraction of a date range (min and max)."""
        df = pl.DataFrame({
            "Date": ["2023-01-01", "2023-01-05", "2023-01-03"]
        })
        result = extract_date_range(df, "Date", "%Y-%m-%d")
        assert result == "2023-01-01~2023-01-05"

    def test_no_data_empty_df(self):
        """Test behavior with empty DataFrame."""
        df = pl.DataFrame({"Date": []})
        result = extract_date_range(df, "Date", "%Y-%m-%d")
        assert result == "no_data"

    def test_no_data_missing_column(self):
        """Test behavior when column is missing."""
        df = pl.DataFrame({"Other": [1, 2, 3]})
        result = extract_date_range(df, "Date", "%Y-%m-%d")
        assert result == "no_data"

    def test_invalid_dates(self):
        """Test behavior with invalid date strings."""
        df = pl.DataFrame({
            "Date": ["invalid", "2023-01-01"]
        })
        # Should ignore invalid and pick valid
        result = extract_date_range(df, "Date", "%Y-%m-%d")
        assert result == "2023-01-01"

    def test_all_invalid_dates(self):
        """Test behavior with all invalid date strings."""
        df = pl.DataFrame({
            "Date": ["invalid", "also invalid"]
        })
        result = extract_date_range(df, "Date", "%Y-%m-%d")
        assert result == "no_data"
