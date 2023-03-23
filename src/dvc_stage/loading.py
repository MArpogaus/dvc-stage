# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-16 12:57:26 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import fnmatch
import logging
import os

import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.utils import import_from_string

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _get_loading_function(format, import_from):
    """
    Get the loading function for a given file-format.

    Args:
        :param format: the file-format to load the data from.
        :type format: str
        :param import_from: module name or path where the custom loading function is located.
        :type import_from: str

    Returns:
        :return: (function): the loading function for the given format.
    """
    if format == "custom":
        fn = import_from_string(import_from)
    elif hasattr(pd, "read_" + format):
        fn = getattr(pd, "read_" + format)
    else:
        raise ValueError(f'loading function for format "{format}" not found')
    return fn


def _get_data_key(path, key_map):
    """
    Private function to get the data key from a file path.

    Args:
        :param path: the file path.
        :type path: str
        :param key_map: a mapping from filename patterns to data keys.
        :type key_map: dict

    Returns:
        :return: the data key associated with the file path.
        :rtype: str
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


# PUBLIC FUNCTIONS ############################################################
def load_data(format, paths, key_map=None, import_from=None, quiet=False, **kwds):
    """
    Load data from one or more files. Executes substage "loading".

    Args:
        :param format: the format to load the data from.
        :type format: str
        :param paths: the file path(s) to load the data from.
        :type paths: str or list
        :param key_map: a mapping from filename patterns to data keys.
        :type key_map: dict
        :param import_from: module name or path where the custom loading function is located.
        :type import_from: str
        :param quiet: whether to disable logging messages or not.
        :type quiet: bool
        :param **kwds: additional keyword arguments to pass to the loading function.
        :type **kwds: object

    Returns:
     :return: (object or dict): the loaded data, either as a single object or a dictionary of objects.
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
            return None
        else:
            __LOGGER__.debug(f"loading data from {paths}")
            fn = _get_loading_function(format, import_from)
            return fn(paths, **kwds)
