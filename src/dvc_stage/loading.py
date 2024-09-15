# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : loading.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 13:51:13 (Marcel Arpogaus)
# changed : 2024-09-15 14:23:53 (Marcel Arpogaus)

# %% Description ###############################################################
"""loading module."""

# %% imports ###################################################################
import fnmatch
import logging
import os
from typing import Union

import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.utils import import_from_string

# %% globals ###################################################################
__LOGGER__ = logging.getLogger(__name__)


# %% private functions #########################################################
def _get_loading_function(format: str, import_from: str) -> callable:
    """Get the loading function for a given file-format.

    Parameters
    ----------
    format : str
        The file-format to load the data from.
    import_from : str
        Module name or path where the custom loading function is located.

    Returns
    -------
    callable
        The loading function for the given format.

    """
    if format == "custom":
        fn = import_from_string(import_from)
    elif hasattr(pd, "read_" + format):
        fn = getattr(pd, "read_" + format)
    else:
        raise ValueError(f'loading function for format "{format}" not found')
    return fn


def _get_data_key(path: str, key_map: dict) -> str:
    """Private function to get the data key from a file path.

    Parameters
    ----------
    path : str
        The file path.
    key_map : dict
        A mapping from filename patterns to data keys.

    Returns
    -------
    str
        The data key associated with the file path.

    """
    k = os.path.basename(path)
    k = os.path.splitext(k)[0]
    if key_map:
        for pat, key in key_map.items():
            match = fnmatch.fnmatch(path, pat)
            if match:
                k = key
                break
    __LOGGER__.debug(f'using key "{k}" for file "{path}"')
    return k


# %% public functions ##########################################################
def load_data(
    format: str,
    paths: Union[str, list],
    key_map: dict = None,
    import_from: str = None,
    quiet: bool = False,
    return_keys: list = False,
    **kwds: object,
) -> Union[object, dict]:
    """Load data from one or more files. Executes substage "loading".

    Parameters
    ----------
    format : str
        The format to load the data from.
    paths : str or list
        The file path(s) to load the data from.
    key_map : dict, optional
        A mapping from filename patterns to data keys.
    import_from : str, optional
        Module name or path where the custom loading function is located.
    quiet : bool, optional
        Whether to disable logging messages or not.
    return_keys: list
        Provide keys in case custom loading functions return s a dict containing
        multiple DataFrames.
    **kwds : object
        Additional keyword arguments to pass to the loading function.

    Returns
    -------
    object or dict
        The loaded data, either as a single object or a dictionary of objects.
    object or dict
        The loaded data, either as a single object or a dictionary of objects.

    """
    __LOGGER__.disabled = quiet
    if len(paths) == 1:
        paths = paths[0]
    if isinstance(paths, list):
        __LOGGER__.debug("got a list of paths")
        data = {}

        with logging_redirect_tqdm():
            it = tqdm(paths, disable=quiet, leave=False)
            for path in it:
                k = _get_data_key(path, key_map)
                __LOGGER__.debug(
                    f"loading data from '{os.path.basename(path)}' as key '{k}'"
                )
                it.set_description(f"loading data as key '{k}'")
                data[k] = load_data(
                    format=format,
                    paths=path,
                    key_map=key_map,
                    import_from=import_from,
                    **kwds,
                )
        return data
    else:
        if format is None:
            if return_keys:
                return dict.fromkeys(return_keys)
            else:
                return None
        else:
            __LOGGER__.debug(f"loading data from {paths}")
            fn = _get_loading_function(format, import_from)
            return fn(paths, **kwds)
