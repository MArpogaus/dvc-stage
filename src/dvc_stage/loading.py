# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-11 07:26:25 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import fnmatch
import importlib
import logging
import os

import pandas as pd
from tqdm import tqdm

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _get_loading_function(format, import_from):
    if format == "custom":
        module_name, function_name = import_from.rsplit(".", 1)
        fn = getattr(importlib.import_module(module_name), function_name)
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
def load_data(format, path, key_map=None, import_from=None, **kwds):
    if len(path) == 1:
        path = path[0]
    if isinstance(path, list):
        __LOGGER__.debug("got a list of paths")
        data = {}
        for p in tqdm(path):
            k = _get_data_key(p, key_map)
            data[k] = load_data(
                format=format, path=p, key_map=key_map, import_from=import_from, **kwds
            )
        return data
    else:
        if format is None:
            return None
        else:
            __LOGGER__.info(f"loading data from {path}")
            fn = _get_loading_function(format, import_from)
            return fn(path, **kwds)
