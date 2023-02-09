# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : transforming.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-24 14:40:39 (Marcel Arpogaus)
# changed : 2023-02-09 16:22:38 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import importlib
import logging
import os
import pickle
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from .common import CUSTOM_MODULE_PREFIX

# MODULE GLOBAL VARIABLES #####################################################
__COLUMN_TRANSFORMER_CACHE__ = {}


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


def _initialize_sklearn_transformer(transformer_class_name, **kwds):
    if transformer_class_name in ("drop", "passthrough"):
        return transformer_class_name
    else:
        transformer_class_pkg, transformer_class_name = transformer_class_name.rsplit(
            ".", 1
        )
        transformer_class = getattr(
            importlib.import_module(transformer_class_pkg), transformer_class_name
        )
        logging.debug(
            f'importing "{transformer_class_name}" from "{transformer_class_pkg}"'
        )
        return transformer_class(**kwds)


def _get_column_transformer(transformers: [], remainder: str = "drop", **kwds):
    from sklearn.compose import make_column_transformer

    column_transformer_key = id(transformers)
    column_transformer = __COLUMN_TRANSFORMER_CACHE__.get(column_transformer_key, None)
    if column_transformer is None:
        transformers = list(
            map(
                lambda trafo: (
                    _initialize_sklearn_transformer(
                        trafo["class_name"], **trafo.get("kwds", {})
                    ),
                    trafo["columns"],
                ),
                transformers,
            )
        )
        column_transformer = make_column_transformer(
            *transformers, remainder=_initialize_sklearn_transformer(remainder), **kwds
        )
        logging.debug(column_transformer)

        __COLUMN_TRANSFORMER_CACHE__[column_transformer_key] = column_transformer

    return column_transformer


def _column_transformer_fit(data: pd.DataFrame, dump_to_file=False, **kwds):
    if data is None:
        return None
    else:
        column_transfomer = _get_column_transformer(**kwds)
        column_transfomer = column_transfomer.fit(data)

        if dump_to_file is not None:
            dirname = os.path.dirname(dump_to_file)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            with open(dump_to_file, "wb+") as file:
                pickle.dump(column_transfomer, file)

        return data


def _column_transformer_transform(data: pd.DataFrame, **kwds):
    if data is None:
        return None
    else:
        column_transfomer = _get_column_transformer(**kwds)
        columns_names = data.columns
        transformed_values = column_transfomer.transform(data)
        data = pd.DataFrame(
            columns=column_transfomer.get_feature_names_out(columns_names),
            data=transformed_values,
        )
        return data


def _get_transformation(data, name):
    if name.startswith(CUSTOM_MODULE_PREFIX):
        module_name, function_name = name.replace(CUSTOM_MODULE_PREFIX, "").rsplit(
            ".", 1
        )
        fn = getattr(importlib.import_module(module_name), function_name)
    elif name in TRANSFORMATION_FUNCTIONS.keys():
        fn = TRANSFORMATION_FUNCTIONS[name]
    elif hasattr(data, name):
        fn = lambda _, **kwds: getattr(data, name)(**kwds)  # noqa E731
    elif data is None and hasattr(pd.DataFrame, name):
        fn = lambda _: None  # noqa E731
    else:
        raise ValueError(f'transformation function "{name}" not found')
    return fn


def _apply_transformation(data, name, exclude=[], include=[], **kwds):
    if isinstance(data, list) and name != "combine":
        logging.debug("arg is list")
        results_list = []
        for idx, dat in tqdm(enumerate(data)):
            if (len(include) == 0 and idx not in exclude) or idx in include:
                logging.debug(f"transforming DataFrame at position {idx}")
                transformed_data = _apply_transformation(
                    data=dat,
                    name=name,
                    **kwds,
                )
            else:
                logging.debug(f"skipping transformation of DataFrame at position {idx}")
                transformed_data = dat
                raise ValueError()
            results_list.append(transformed_data)
        return results_list
    if isinstance(data, dict):
        logging.debug("arg is dict")
        results_dict = {}
        for key, dat in tqdm(data.items()):
            if (len(include) == 0 and key not in exclude) or key in include:
                logging.debug(f"transforming DataFrame with key {key}")
                transformed_data = _apply_transformation(
                    data=dat,
                    name=name,
                    **kwds,
                )
            else:
                logging.debug(f"skipping transformation of DataFrame with key {key}")
                transformed_data = dat
            results_dict[key] = transformed_data
        return results_dict
    else:
        logging.debug(f"applying transformation: {name}")
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
TRANSFORMATION_FUNCTIONS = {
    "split": _split,
    "combine": _combine,
    "column_transformer_fit": _column_transformer_fit,
    "column_transformer_transform": _column_transformer_transform,
}
