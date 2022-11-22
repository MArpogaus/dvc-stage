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
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm


# FUNCTION DEFINITIONS ########################################################
def date_time_split(
    data: pd.DataFrame, size: float, freq: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """split data along date time axis

    NOTE: Only tested for Monthly splits so far

    :param data: data to split
    :type data: pd.DataFrame
    :param size: amount of time steps
    :type size: float
    :pram freq: frequency to split on
    :type freq: str
    :returns: Tuple[pd.DataFrame, pd.DataFrame]

    """
    start_point = data.date_time.dt.date.min()
    end_date = data.date_time.dt.date.max()

    data.set_index("date_time", inplace=True)

    # Reserve some data for testing
    periods = len(pd.period_range(start_point, end_date, freq=freq))
    split_point = start_point + int(np.round(size * periods)) * pd.offsets.MonthBegin()
    logging.info(
        f"left split from {start_point} till {split_point - pd.offsets.Minute(30)}"
    )
    logging.info(f"right split from {split_point} till {end_date}")

    left_split_str = str(split_point - pd.offsets.Minute(30))
    right_split_str = str(split_point)
    left_data = data.loc[:left_split_str]
    right_data = data.loc[right_split_str:]

    return left_data, right_data


def id_split(
    data: pd.DataFrame, size: float, seed: int, id_col: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """split data on a random set of ids

    :param data: data to split
    :type data: pd.DataFrame
    :param size: amount of random ids in the left split
    :type size: float
    :param seed: seed used for id shuffling
    :type seed: int
    :param id_col: column containing id information
    :type id_col: str
    :returns: Tuple[pd.DataFrame, pd.DataFrame]

    """
    np.random.seed(seed)
    ids = list(sorted(data[id_col].unique()))
    np.random.shuffle(ids)
    ids = ids[: int(size * len(ids))]
    mask = data[id_col].isin(ids)
    return data[mask], data[~mask]


def split(
    data: pd.DataFrame, by: str, left_split_name: str, right_split_name: str, **kwds
) -> Dict[str, pd.DataFrame]:
    """TODO describe function

    :param data: data to split
    :type data: pd.DataFrame
    :param by: type of split
    :type by: str
    :param left_split_name: name for left split
    :type left_split_name: str
    :param right_split_name: name for right split
    :type right_split_name: str
    :returns:

    """
    if data is None:
        logging.debug("tracing split function")
        return {left_split_name: None, right_split_name: None}
    else:
        if by == "id":
            left_split, right_split = id_split(data, **kwds)
        elif by == "date_time":
            left_split, right_split = date_time_split(data, **kwds)
        else:
            raise ValueError(f"invalid choice for split: {by}")

        return {left_split_name: left_split, right_split_name: right_split}


def apply_transformation(name, arg, **kwds):
    if isinstance(arg, list):
        logging.debug("arg is list")
        l = []
        for a in tqdm(arg):
            l.append(apply_transformation(name, a, **kwds))
        return l
    if isinstance(arg, dict):
        logging.debug("arg is dict")
        d = {}
        for k, v in tqdm(arg.items()):
            d[k] = apply_transformation(name, a, **kwds)
        return d
    else:
        logging.debug(f"applying {name}")
        fn = TRANSFORMATION_FUNCTIONS[name]
        return fn(arg, **kwds)


# GLOBAL VARIABLES ############################################################
TRANSFORMATION_FUNCTIONS = {"split": split}
