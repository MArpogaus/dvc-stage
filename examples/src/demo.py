"""Demonstration module."""

from __future__ import annotations

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index


def duplicate(data):
    """Duplicate rows in dataframe with continuing index."""
    # raise Exception(f"data: {data}")
    if data is None:
        return None
    return pd.concat([data, data], ignore_index=True)


def isNotNone(data):
    """Check if data is None."""
    return data is not None


def normalize_data(data: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Normalize specified columns using min-max scaling.

    Parameters
    ----------
    data : pd.DataFrame
        Input dataframe to normalize.
    columns : list[str]
        List of column names to normalize.

    Returns
    -------
    pd.DataFrame
        Dataframe with normalized columns.

    """
    if data is None:
        return None

    result = data.copy()
    for col in columns:
        if col in result.columns:
            min_val = result[col].min()
            max_val = result[col].max()
            if max_val > min_val:
                result[col] = (result[col] - min_val) / (max_val - min_val)

    return result


def check_data_quality(
    data: pd.DataFrame | dict[str, pd.DataFrame], min_rows: int = 10
) -> bool:
    """Check if data meets minimum quality requirements.

    Parameters
    ----------
    data : pd.DataFrame | dict[str, pd.DataFrame]
        Input data to validate.
    min_rows : int, optional
        Minimum number of rows required. Default is 10.

    Returns
    -------
    bool
        True if data meets quality requirements.

    """
    if data is None:
        return False

    if isinstance(data, dict):
        return all(df is not None and len(df) >= min_rows for df in data.values())
    else:
        return len(data) >= min_rows


def validate_split_ratio(
    data: dict[str, pd.DataFrame],
    expected_ratio: float = 0.8,
    tolerance: float = 0.05,
    **_: dict,
) -> bool:
    """Validate that train/test split ratio is within expected range.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        Dictionary containing 'train' and 'test' dataframes.
    expected_ratio : float, optional
        Expected ratio of train data. Default is 0.8.
    tolerance : float, optional
        Acceptable tolerance for ratio. Default is 0.05.
    **_ : dict
        Additional unused keyword arguments.

    Returns
    -------
    bool
        True if split ratio is within tolerance.

    """
    if data is None or "train" not in data or "test" not in data:
        return False

    train_size = len(data["train"])
    test_size = len(data["test"])
    total_size = train_size + test_size

    if total_size == 0:
        return False

    actual_ratio = train_size / total_size
    return abs(actual_ratio - expected_ratio) <= tolerance


def get_schema():
    """Return Pandera Schema for Demo Data."""
    return DataFrameSchema(
        columns={
            "O1": Column(
                str,
                checks=[Check(lambda s: s.str is not None and s.str != "")],
            ),
            "O2": Column(
                str,
                checks=[Check(lambda s: s.str is not None and s.str != "")],
            ),
            "D1": Column(
                str,
                checks=[Check(lambda s: s.str is not None and s.str != "")],
            ),
            "D2": Column(
                str,
                checks=[Check(lambda s: s.str is not None and s.str != "")],
            ),
        },
        index=Index(
            dtype="str",
        ),
        strict=True,
    )


def get_advanced_schema():
    """Get advanced schema for multi-input pipeline validation."""
    return DataFrameSchema(
        {
            "feature1": Column(float, nullable=False),
            "feature2": Column(float, nullable=False),
            "category": Column(str, nullable=False),
            "target": Column(float, nullable=False),
        }
    )


def get_foreach_schema():
    """Get schema for foreach pipeline validation."""
    return DataFrameSchema(
        {
            "value1": Column(float, nullable=False),
            "value2": Column(float, nullable=False),
            "category": Column(str, nullable=False),
        }
    )


def get_timeseries_schema():
    """Get schema for timeseries data validation."""
    return DataFrameSchema(
        {
            "value": Column(float, nullable=False),
        },
        index=Index("datetime64[ns]", nullable=False),
    )
