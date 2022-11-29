# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2022-11-23 12:40:52 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import glob
import logging

import pandas as pd
from tqdm import tqdm


# FUNCTION DEFINITIONS ########################################################
def load_feather(path: str) -> pd.DataFrame:
    """load data from feather file

    :param path: path to feather file
    :type path: str
    :returns: pd.DataFrame

    """
    logging.info(f"loading data from {path}")
    data = pd.read_feather(path)
    return data


def expand_if_glob(path):
    if "*" in path:
        return glob.glob(path)
    else:
        return path


def load_data(format, path, custom_data_load_functions, **kwds):
    path = expand_if_glob(path)
    if isinstance(path, list):
        logging.debug("got a list of paths")
        data = []
        for p in tqdm(path):
            data.append(
                load_data(
                    format=format,
                    path=p,
                    custom_data_load_functions=custom_data_load_functions,
                )
            )
        return data
    else:
        return custom_data_load_functions.get(format, DATA_LOAD_FUNCTIONS[format])(
            path, **kwds
        )


# GLOBAL VARIABLES ############################################################
DATA_LOAD_FUNCTIONS = {"feather": load_feather}
