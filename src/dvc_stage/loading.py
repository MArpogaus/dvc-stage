# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2022-11-22 14:52:54 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import logging

import pandas as pd


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


def load_data(format, path, **kwds):
    return DATA_LOAD_FUNCTIONS[format](path, **kwds)


# GLOBAL VARIABLES ############################################################
DATA_LOAD_FUNCTIONS = {"feather": load_feather}
