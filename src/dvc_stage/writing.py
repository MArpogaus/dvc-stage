# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : writing.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 13:56:17 (Marcel Arpogaus)
# changed : 2025-06-20 14:56:28 (Marcel Arpogaus)

# %% Description ###############################################################
"""Module defining data writing functions."""

# %% imports ###################################################################
from __future__ import annotations

import logging
import os

import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.utils import import_from_string, parse_path

# %% globals ###################################################################
__LOGGER__ = logging.getLogger(__name__)


# %% private functions #########################################################
def _get_writing_function(data: any, format: str, import_from: str | None) -> callable:
    """Return a writing function for a given data format.

    Parameters
    ----------
    data : any
        The data to be written.
    format : str
        The format to write the data in.
    import_from : str | None
        The module path for the custom writing function. Default is None.

    Returns
    -------
    callable
        The writing function.

    Raises
    ------
    ValueError
        If the writing function for the given format is not found.

    """
    if format == "custom":
        fn = import_from_string(import_from)
    elif hasattr(data, "to_" + format):
        fn = lambda _, path: getattr(data, "to_" + format)(path)  # noqa E731
    else:
        raise ValueError(f'writing function for format "{format}" not found')
    return fn


# %% public functions ##########################################################
def write_data(
    data: pd.DataFrame | dict[str, pd.DataFrame],
    format: str,
    path: str,
    import_from: str | None = None,
    item: str | None = None,
    **kwds: any,
) -> None:
    """Write data to a file. Main entrypoint for writing substage.

    Parameters
    ----------
    data : pd.DataFrame | dict[str, pd.DataFrame]
        The data to be written.
    format : str
        The format of the output file.
    path : str
        The path to write the file to.
    import_from : str | None, optional
        The module path of a custom writing function. Default is None.
    item : str | None, optional
        Item identifier for foreach stages. Default is None.
    **kwds : any
        Additional keyword arguments passed to the writing function.

    """
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    if isinstance(data, dict):
        __LOGGER__.debug("arg is dict")
        it = tqdm(data.items(), leave=False)
        with logging_redirect_tqdm():
            for k, v in it:
                formatted_path = parse_path(path, key=k, item=item)[0]
                __LOGGER__.debug(f"writing df with key {k} to '{formatted_path}'")
                it.set_description(f"writing df with key {k}")
                write_data(
                    format=format,
                    data=v,
                    path=formatted_path,
                )
    else:
        __LOGGER__.debug(f"saving data to {path} as {format}")
        fn = _get_writing_function(data, format, import_from)
        fn(data, path, **kwds)
