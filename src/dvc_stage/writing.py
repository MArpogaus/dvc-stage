# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : writing.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 13:56:17 (Marcel Arpogaus)
# changed : 2024-09-15 14:16:33 (Marcel Arpogaus)

# %% Description ###############################################################
"""Module defining data writing functions."""

# %% imports ###################################################################
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.utils import import_from_string

# %% globals ###################################################################
__LOGGER__ = logging.getLogger(__name__)


# %% private functions #########################################################
def _get_writing_function(
    data: Any, format: str, import_from: Optional[str]
) -> Callable:
    """Return a writing function for a given data format.

    Parameters
    ----------
    data : Any
        The data to be written.
    format : str
        The format to write the data in.
    import_from : Optional[str]
        The module path for the custom writing function (default: None).

    Returns
    -------
    Callable
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
    data: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
    format: str,
    path: str,
    import_from: Optional[str] = None,
    **kwds: Any,
) -> None:
    """Write data to a file. Main entrypoint for writing substage.

    Parameters
    ----------
    data : Union[pd.DataFrame, Dict[str, pd.DataFrame]]
        The data to be written.
    format : str
        The format of the output file.
    path : str
        The path to write the file to.
    import_from : Optional[str], optional
        The module path of a custom writing function, by default None.
    kwds : Any
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
                formatted_path = path.format(key=k)
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


def get_outs(
    data: Union[List, Dict, pd.DataFrame], path: str, **kwds: Any
) -> List[str]:
    """Get list of output paths based on input data.

    Parameters
    ----------
    data : Union[List, Dict, pd.DataFrame]
        Input data.
    path : str
        Output path template string.
    kwds : Any
        Additional keyword arguments.

    Returns
    -------
    List[str]
        List of output paths.

    """
    outs = []

    if isinstance(data, list):
        __LOGGER__.debug("data is list")
        for i, d in enumerate(data):
            outs.append(path.format(item=i))
    elif isinstance(data, dict):
        __LOGGER__.debug("arg is dict")
        for k, v in data.items():
            outs.append(path.format(key=k))
    else:
        __LOGGER__.debug(f"path: {path}")
        outs.append(path)

    return list(sorted(outs))
