# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-10 15:47:23 (Marcel Arpogaus)
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


# PRIVATE FUNCTIONS ###########################################################
def _get_loading_function(format, import_from):
    if format == "custom":
        module_name, function_name = import_from.rsplit(".", 1)
        fn = getattr(importlib.import_module(module_name), function_name)
    elif hasattr(pd, format):
        fn = getattr(pd, "read_" + format)
    else:
        raise ValueError(f'loading function for format "{format}" not found')
    return fn


# PUBLIC FUNCTIONS ############################################################
def load_data(format, path, as_dict=False, key_map=None, import_from=None, **kwds):
    if len(path) == 1:
        path = path[0]
    if isinstance(path, list):
        logging.debug("got a list of paths")
        if as_dict:
            data = {}
            for p in tqdm(path):
                k = None
                if key_map:
                    for pat, key in key_map.items():
                        match = fnmatch.fnmatch(p, pat)
                        if match:
                            k = key
                            print(k)
                            break
                    if k is None:
                        raise ValueError(f"Could not find matching key for '{pat}'")
                else:
                    k = os.path.basename(p)
                    k = os.path.splitext(k)[0]
                data[k] = load_data(
                    format=format, path=p, as_dict=as_dict, import_from=import_from
                )
        else:
            data = []
            for p in tqdm(path):
                data.append(
                    load_data(
                        format=format, path=p, as_dict=as_dict, import_from=import_from
                    )
                )
        return data
    else:
        if format is None:
            return None
        else:
            logging.info(f"loading data from {path}")
            fn = _get_loading_function(format, import_from)
            return fn(path, **kwds)
