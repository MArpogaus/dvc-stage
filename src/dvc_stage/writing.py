# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2022-11-23 11:19:13 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import logging
import os

import pandas as pd
from tqdm import tqdm


# FUNCTION DEFINITIONS ########################################################
def save_feather(data: pd.DataFrame, path: str) -> None:
    """save data to feather file

    :param data: data to save
    :type data: pd.DataFrame
    :param path: path to feather file
    :type path: str

    """
    logging.info(f"writing data to {path}")
    data.reset_index(drop=True).to_feather(path)


def write_data(format, data, path, custom_data_write_functions, **kwds):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    if isinstance(data, list):
        logging.debug("data is list")
        for i, d in tqdm(enumerate(data)):
            write_data(
                format=format,
                data=d,
                path=path.format(item=i),
                custom_data_write_functions=custom_data_write_functions,
            )
    if isinstance(data, dict):
        logging.debug("arg is dict")
        for k, v in tqdm(data.items()):
            write_data(
                format=format,
                data=v,
                path=path.format(key=k),
                custom_data_write_functions=custom_data_write_functions,
            )
    else:
        fn = custom_data_write_functions.get(format, DATA_WRITE_FUNCTIONS[format])
        logging.debug(f"saving data to {path} as {format}")
        fn(data, path, **kwds)


# GLOBAL VARIABLES ############################################################
DATA_WRITE_FUNCTIONS = {"feather": save_feather}
