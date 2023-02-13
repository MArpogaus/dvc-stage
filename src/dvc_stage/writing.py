# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-13 15:04:51 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import importlib
import logging
import os

from tqdm import tqdm

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _get_writing_function(data, format, import_from):
    if format == "custom":
        module_name, function_name = import_from.rsplit(".", 1)
        fn = getattr(importlib.import_module(module_name), function_name)
    elif hasattr(data, "to_" + format):
        fn = lambda _, path: getattr(data, "to_" + format)(path)  # noqa E731
    else:
        raise ValueError(f'writing function for format "{format}" not found')
    return fn


# PUBLIC FUNCTIONS ############################################################
def write_data(data, format, path, import_from=None, **kwds):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    if isinstance(data, dict):
        __LOGGER__.debug("arg is dict")
        for k, v in tqdm(data.items()):
            write_data(
                format=format,
                data=v,
                path=path.format(key=k),
            )
    else:
        __LOGGER__.debug(f"saving data to {path} as {format}")
        fn = _get_writing_function(data, format, import_from)
        fn(data, path, **kwds)


def get_outs(data, path, **kwds):
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
        outs.insert(path)

    return list(sorted(outs))
