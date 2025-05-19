"""Demonstation module."""

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
