# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2022-12-13 14:58:14 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import glob
import logging
import os

import pandas as pd
from tqdm import tqdm


# PRIVATE FUNCTIONS ###########################################################
def _load_feather(path: str) -> pd.DataFrame:
    """load data from feather file

    :param path: path to feather file
    :type path: str
    :returns: pd.DataFrame

    """
    logging.info(f"loading data from {path}")
    data = pd.read_feather(path)
    return data


# PUBLIC FUNCTIONS ############################################################
def get_deps(path):
    deps = []
    if isinstance(path, list):
        for p in path:
            deps += get_deps(p)
        deps = list(sorted(set(deps)))
    else:
        deps = glob.glob(path)

    assert (
        len(deps) > 0
    ), f'Dependencies not found for path "{path}".\nIs DVC Pipeline up to date?'
    return deps


def load_data(format, path, as_dict=False, **kwds):
    path = get_deps(path)
    if len(path) == 1:
        path = path[0]
    if isinstance(path, list):
        logging.debug("got a list of paths")
        if as_dict:
            data = {}
            for p in tqdm(path):
                k = os.path.basename(p)
                data[k] = load_data(
                    format=format,
                    path=p,
                )
        else:
            data = []
            for p in tqdm(path):
                data.append(
                    load_data(
                        format=format,
                        path=p,
                    )
                )
        return data
    else:
        return DATA_LOAD_FUNCTIONS[format](path, **kwds)


# GLOBAL VARIABLES ############################################################
DATA_LOAD_FUNCTIONS = {"feather": _load_feather}
