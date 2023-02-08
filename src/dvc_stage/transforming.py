# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : transforming.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-24 14:40:39 (Marcel Arpogaus)
# changed : 2023-02-08 12:18:45 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import logging
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


# PRIVATE FUNCTIONS ###########################################################
def _date_time_split(
    data: pd.DataFrame, size: float, freq: str, date_time_col: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """split data along date time axis

    NOTE: Only tested for Monthly splits so far

    :param data: data to split
    :type data: pd.DataFrame
    :param size: amount of time steps
    :type size: float
    :pram freq: frequency to split on
    :type freq: str
    :pram date_time_col: column containing the date time index
    :type date_time_col: str
    :returns: Tuple[pd.DataFrame, pd.DataFrame]

    """
    start_point = data[date_time_col].dt.date.min()
    end_date = data[date_time_col].dt.date.max()

    data.set_index(date_time_col, inplace=True)

    # Reserve some data for testing
    periods = len(pd.period_range(start_point, end_date, freq=freq))
    split_point = start_point + int(np.round(size * periods)) * pd.offsets.MonthBegin()
    logging.info(
        f"left split from {start_point} till {split_point - pd.offsets.Minute(30)}"
    )
    logging.info(f"right split from {split_point} till {end_date}")

    left_split_str = str(split_point - pd.offsets.Minute(30))
    right_split_str = str(split_point)
    left_data = data.loc[:left_split_str].reset_index()
    right_data = data.loc[right_split_str:].reset_index()

    return left_data, right_data


def _id_split(
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


def _split(
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
            left_split, right_split = _id_split(data, **kwds)
        elif by == "date_time":
            left_split, right_split = _date_time_split(data, **kwds)
        else:
            raise ValueError(f"invalid choice for split: {by}")

        return {left_split_name: left_split, right_split_name: right_split}


def _combine(data: List[pd.DataFrame]) -> pd.DataFrame:
    if data[0] is None:
        return None
    else:
        df_combined = data[0]
        for df in tqdm(data[1:]):
            df_combined.append(df)
        return df_combined


def _get_transformation(data, name):
    fn = TRANSFORMATION_FUNCTIONS.get(name)
    if fn is None:
        if hasattr(data, name):
            fn = lambda _, **kwds: getattr(data, name)(**kwds)  # noqa E731
        elif data is None and hasattr(pd.DataFrame, name):
            fn = lambda _: None  # noqa E731
    return fn


def _apply_transformation(data, name, **kwds):
    if isinstance(data, list) and name != "combine":
        logging.debug("arg is list")
        results_list = []
        for a in tqdm(data):
            results_list.append(
                _apply_transformation(
                    data=a,
                    name=name,
                    **kwds,
                )
            )
        return results_list
    if isinstance(data, dict):
        logging.debug("arg is dict")
        results_dict = {}
        for k, v in tqdm(data.items()):
            results_dict[k] = _apply_transformation(
                data=v,
                name=name,
                **kwds,
            )
        return results_dict
    else:
        logging.debug(f"applying {name}")
        fn = _get_transformation(data, name)
        return fn(data, **kwds)


# PUBLIC FUNCTIONS ############################################################
def apply_transformations(data, transformations):
    logging.debug("applying transformations")
    logging.debug(transformations)
    it = tqdm(transformations.items())
    with logging_redirect_tqdm():
        for name, kwds in it:
            it.set_description(name)
            data = _apply_transformation(
                data=data,
                name=name,
                **kwds,
            )
    return data


# GLOBAL VARIABLES ############################################################
TRANSFORMATION_FUNCTIONS = {"split": _split, "combine": _combine}
