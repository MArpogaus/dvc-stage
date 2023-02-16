# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-16 09:22:36 (Marcel Arpogaus)
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

from dvc_stage.utils import import_from_string

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _get_loading_function(format, import_from):
    if format == "custom":
        fn = import_from_string(import_from)
    elif hasattr(pd, "read_" + format):
        fn = getattr(pd, "read_" + format)
    else:
        raise ValueError(f'loading function for format "{format}" not found')
    return fn


def _get_data_key(path, key_map):
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
    __LOGGER__.disabled = quiet
    if len(paths) == 1:
        paths = paths[0]
    if isinstance(paths, list):
        __LOGGER__.debug("got a list of paths")
        data = {}
        if quiet:
            it = paths
        else:
            it = tqdm(paths)
        for p in it:
            k = _get_data_key(p, key_map)
            data[k] = load_data(
                format=format, paths=p, key_map=key_map, import_from=import_from, **kwds
            )
        return data
    else:
        if format is None:
            return None
        else:
            __LOGGER__.debug(f"loading data from {paths}")
            fn = _get_loading_function(format, import_from)
            return fn(paths, **kwds)
