"""Pytest configuration and fixtures."""

import pandas as pd
import pytest


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "feature1": [1.0, 2.0, 3.0, 4.0],
            "feature2": [0.5, 1.5, 2.5, 3.5],
            "feature3": [10, 20, 30, 40],
            "category": ["A", "B", "A", "B"],
            "target": [0, 1, 0, 1],
        }
    )


@pytest.fixture
def sample_params():
    """Create sample parameters for testing."""
    return {"param1": "test_value", "param2": "another_value", "log_level": "info"}
