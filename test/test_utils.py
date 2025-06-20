"""Tests for the utils module."""

import pytest

from dvc_stage.utils import flatten_dict, import_from_string, key_is_skipped, parse_path


class TestParsePathFunction:
    """Test cases for parse_path function."""

    def test_parse_path_no_placeholders(self):
        """Test parsing path without placeholders."""
        path = "data/input.csv"
        result_path, matches = parse_path(path, param1="value1")
        assert result_path == "data/input.csv"
        assert matches == set()

    def test_parse_path_with_placeholders(self):
        """Test parsing path with placeholders."""
        path = "data/${param1}/input.csv"
        result_path, matches = parse_path(path, param1="test")
        assert result_path == "data/test/input.csv"
        assert matches == {"param1"}

    def test_parse_path_with_item_placeholder(self):
        """Test parsing path with item placeholder."""
        path = "data/${item}/input.csv"
        result_path, matches = parse_path(path, item="dataset1")
        assert result_path == "data/dataset1/input.csv"
        assert matches == {"item"}

    def test_parse_path_item_placeholder_no_value(self):
        """Test parsing path with item placeholder but no value."""
        path = "data/${item}/input.csv"
        result_path, matches = parse_path(path)
        assert result_path == "data/${item}/input.csv"
        assert matches == {"item"}


class TestFlattenDictFunction:
    """Test cases for flatten_dict function."""

    def test_flatten_dict_simple(self):
        """Test flattening simple nested dictionary."""
        d = {"a": {"b": 1, "c": 2}, "d": 3}
        result = flatten_dict(d)
        expected = {"a.b": 1, "a.c": 2, "d": 3}
        assert result == expected

    def test_flatten_dict_deep_nesting(self):
        """Test flattening deeply nested dictionary."""
        d = {"a": {"b": {"c": 1}}, "d": 2}
        result = flatten_dict(d)
        expected = {"a.b.c": 1, "d": 2}
        assert result == expected

    def test_flatten_dict_custom_separator(self):
        """Test flattening with custom separator."""
        d = {"a": {"b": 1}, "c": 2}
        result = flatten_dict(d, sep="_")
        expected = {"a_b": 1, "c": 2}
        assert result == expected


class TestImportFromStringFunction:
    """Test cases for import_from_string function."""

    def test_import_from_string_builtin(self):
        """Test importing builtin function."""
        func = import_from_string("builtins.len")
        assert func == len

    def test_import_from_string_invalid(self):
        """Test importing invalid function."""
        with pytest.raises(ModuleNotFoundError):
            import_from_string("nonexistent.module.function")


class TestKeyIsSkippedFunction:
    """Test cases for key_is_skipped function."""

    def test_key_is_skipped_no_filters(self):
        """Test key skipping with no filters."""
        assert not key_is_skipped("test_key", [], [])

    def test_key_is_skipped_include_filter(self):
        """Test key skipping with include filter."""
        assert not key_is_skipped("test_key", ["test_key"], [])
        assert key_is_skipped("other_key", ["test_key"], [])

    def test_key_is_skipped_exclude_filter(self):
        """Test key skipping with exclude filter."""
        assert key_is_skipped("test_key", [], ["test_key"])
        assert not key_is_skipped("other_key", [], ["test_key"])
