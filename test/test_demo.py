"""Tests for the demo module functions."""

import pandas as pd
from demo import (
    check_data_quality,
    duplicate,
    get_advanced_schema,
    get_schema,
    get_timeseries_schema,
    isNotNone,
    normalize_data,
    validate_split_ratio,
)


class TestDemoFunctions:
    """Test cases for demo module functions."""

    def test_duplicate_with_data(self):
        """Test duplicate function with valid data."""
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        result = duplicate(df)
        expected = pd.DataFrame({"A": [1, 2, 1, 2], "B": [3, 4, 3, 4]})
        pd.testing.assert_frame_equal(result, expected)

    def test_duplicate_with_none(self):
        """Test duplicate function with None input."""
        result = duplicate(None)
        assert result is None

    def test_isNotNone_with_data(self):
        """Test isNotNone function with valid data."""
        df = pd.DataFrame({"A": [1, 2]})
        assert isNotNone(df) is True

    def test_isNotNone_with_none(self):
        """Test isNotNone function with None input."""
        assert isNotNone(None) is False

    def test_normalize_data_with_valid_data(self):
        """Test normalize_data function with valid data."""
        df = pd.DataFrame({"A": [1, 2, 3, 4], "B": [10, 20, 30, 40]})
        result = normalize_data(df, ["A", "B"])

        # Check that values are normalized between 0 and 1
        assert result["A"].min() == 0.0
        assert result["A"].max() == 1.0
        assert result["B"].min() == 0.0
        assert result["B"].max() == 1.0

    def test_normalize_data_with_none(self):
        """Test normalize_data function with None input."""
        result = normalize_data(None, ["A"])
        assert result is None

    def test_normalize_data_with_nonexistent_columns(self):
        """Test normalize_data function with nonexistent columns."""
        df = pd.DataFrame({"A": [1, 2, 3]})
        result = normalize_data(df, ["B"])  # Column B doesn't exist
        pd.testing.assert_frame_equal(result, df)  # Should return unchanged

    def test_check_data_quality_dataframe_sufficient_rows(self):
        """Test check_data_quality with DataFrame having sufficient rows."""
        df = pd.DataFrame({"A": range(15)})  # 15 rows > 10 min_rows
        assert check_data_quality(df, min_rows=10) is True

    def test_check_data_quality_dataframe_insufficient_rows(self):
        """Test check_data_quality with DataFrame having insufficient rows."""
        df = pd.DataFrame({"A": range(5)})  # 5 rows < 10 min_rows
        assert check_data_quality(df, min_rows=10) is False

    def test_check_data_quality_dict_sufficient_rows(self):
        """Test check_data_quality with dict having sufficient rows."""
        data = {
            "train": pd.DataFrame({"A": range(15)}),
            "test": pd.DataFrame({"A": range(12)}),
        }
        assert check_data_quality(data, min_rows=10) is True

    def test_check_data_quality_dict_insufficient_rows(self):
        """Test check_data_quality with dict having insufficient rows."""
        data = {
            "train": pd.DataFrame({"A": range(15)}),
            "test": pd.DataFrame({"A": range(5)}),  # 5 rows < 10 min_rows
        }
        assert check_data_quality(data, min_rows=10) is False

    def test_check_data_quality_with_none(self):
        """Test check_data_quality with None input."""
        assert check_data_quality(None) is False

    def test_validate_split_ratio_valid_ratio(self):
        """Test validate_split_ratio with valid ratio."""
        data = {
            "train": pd.DataFrame({"A": range(80)}),  # 80 rows
            "test": pd.DataFrame({"A": range(20)}),  # 20 rows
        }
        # 80/100 = 0.8, which matches expected_ratio=0.8
        assert validate_split_ratio(data, expected_ratio=0.8, tolerance=0.05) is True

    def test_validate_split_ratio_invalid_ratio(self):
        """Test validate_split_ratio with invalid ratio."""
        data = {
            "train": pd.DataFrame({"A": range(60)}),  # 60 rows
            "test": pd.DataFrame({"A": range(40)}),  # 40 rows
        }
        # 60/100 = 0.6, which is outside tolerance of 0.8 ± 0.05
        assert validate_split_ratio(data, expected_ratio=0.8, tolerance=0.05) is False

    def test_validate_split_ratio_missing_keys(self):
        """Test validate_split_ratio with missing keys."""
        data = {"train": pd.DataFrame({"A": range(80)})}  # Missing 'test'
        assert validate_split_ratio(data) is False

    def test_validate_split_ratio_with_none(self):
        """Test validate_split_ratio with None input."""
        assert validate_split_ratio(None) is False

    def test_validate_split_ratio_empty_data(self):
        """Test validate_split_ratio with empty dataframes."""
        data = {"train": pd.DataFrame(), "test": pd.DataFrame()}
        assert validate_split_ratio(data) is False

    def test_get_schema_returns_dataframe_schema(self):
        """Test that get_schema returns a valid DataFrameSchema."""
        schema = get_schema()
        if schema is not None:  # Only test if pandera is available
            assert hasattr(schema, "columns")
            assert "O1" in schema.columns
            assert "O2" in schema.columns
            assert "D1" in schema.columns
            assert "D2" in schema.columns

    def test_get_advanced_schema_returns_dataframe_schema(self):
        """Test that get_advanced_schema returns a valid DataFrameSchema."""
        schema = get_advanced_schema()
        if schema is not None:  # Only test if pandera is available
            assert hasattr(schema, "columns")
            assert "feature1" in schema.columns
            assert "feature2" in schema.columns
            assert "category" in schema.columns
            assert "target" in schema.columns

    def test_get_timeseries_schema_returns_dataframe_schema(self):
        """Test that get_timeseries_schema returns a valid DataFrameSchema."""
        schema = get_timeseries_schema()
        if schema is not None:  # Only test if pandera is available
            assert hasattr(schema, "columns")
            assert "value" in schema.columns
            assert hasattr(schema, "index")
