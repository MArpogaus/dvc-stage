# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : transforming.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-24 14:40:39 (Marcel Arpogaus)
# changed : 2023-02-20 16:19:03 (Marcel Arpogaus)
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

from dvc_stage.utils import import_from_string, key_is_skipped

# MODULE GLOBAL VARIABLES #####################################################
__COLUMN_TRANSFORMER_CACHE__ = {}
__LOGGER__ = logging.getLogger(__name__)


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
    __LOGGER__.debug(
        f"left split from {start_point} till {split_point - pd.offsets.Minute(30)}"
    )
    __LOGGER__.debug(f"right split from {split_point} till {end_date}")

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
        __LOGGER__.debug(
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
        __LOGGER__.debug(column_transformer)

        __COLUMN_TRANSFORMER_CACHE__[column_transformer_key] = column_transformer

    return column_transformer


def _get_transformation(data, id, import_from):
    if id == "custom":
        fn = import_from_string(import_from)
    elif id in globals().keys():
        fn = globals()[id]
    elif hasattr(data, id):
        fn = lambda _, **kwds: getattr(data, id)(**kwds)  # noqa E731
    elif data is None and hasattr(pd.DataFrame, id):
        fn = lambda _, **__: None  # noqa E731
    else:
        raise ValueError(f'transformation function "{id}" not found')
    return fn


def _apply_transformation(
    data,
    id: List[str],
    import_from=None,
    exclude=[],
    include=[],
    quiet=False,
    pass_key_to_fn=False,
    **kwds,
):
    __LOGGER__.disabled = quiet
    if isinstance(data, dict) and id != "combine":
        __LOGGER__.debug("arg is dict")
        results_dict = {}
        it = tqdm(data.items(), disable=quiet, leave=False)
        for key, dat in it:
            description = f"transforming df with key '{key}'"
            __LOGGER__.debug(description)
            it.set_description(description)
            if key_is_skipped(key, include, exclude):
                __LOGGER__.debug(f"skipping transformation of DataFrame with key {key}")
                transformed_data = dat
            else:
                __LOGGER__.debug(f"transforming DataFrame with key {key}")
                if pass_key_to_fn:
                    kwds.update({"key": key})
                transformed_data = _apply_transformation(
                    data=dat,
                    id=id,
                    import_from=import_from,
                    exclude=exclude,
                    include=include,
                    quiet=quiet,
                    **kwds,
                )
            if isinstance(transformed_data, dict):
                results_dict.update(transformed_data)
            else:
                results_dict[key] = transformed_data
        it.set_description("all transformations applied")
        return results_dict
    elif isinstance(data, dict) and id == "combine":
        __LOGGER__.debug("Combining data")
        return combine(data, include, exclude, **kwds)
    else:
        __LOGGER__.debug(f"applying transformation: {id}")
        fn = _get_transformation(data, id, import_from)
        return fn(data, **kwds)


# PUBLIC FUNCTIONS ############################################################
def split(
    data: pd.DataFrame, by: str, left_split_key: str, right_split_key: str, **kwds
) -> Dict[str, pd.DataFrame]:
    """split data along index

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
        __LOGGER__.debug("tracing split function")
        return {left_split_key: None, right_split_key: None}
    else:
        if by == "id":
            left_split, right_split = _id_split(data, **kwds)
        elif by == "date_time":
            left_split, right_split = _date_time_split(data, **kwds)
        else:
            raise ValueError(f"invalid choice for split: {by}")

        return {left_split_key: left_split, right_split_key: right_split}


def combine(
    data: Dict[str, pd.DataFrame],
    include: List[str],
    exclude: List[str],
    new_key: str = "combined",
) -> List[pd.DataFrame]:
    """concatenate multiple DataFrames

    :param data: dict with data frames to concatenate
    :type data: Dict[pd.DataFrame]
    :param include: keys to include
    :type include: List[str]
    :param exclude: keys to exclude
    :type exclude: List[str]
    :param new_key: new key for concatenated data
    :type new_key: str
    :returns:

    """
    to_combine = []
    for key in list(data.keys()):
        if not key_is_skipped(key, include, exclude):
            to_combine.append(data.pop(key))

    if to_combine[0] is None:
        combined = None
    else:
        combined = pd.concat(to_combine)

    if len(data) > 0:
        data[new_key] = combined
    else:
        data = combined

    return data


def column_transformer_fit(data: pd.DataFrame, dump_to_file: str = None, **kwds):
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


def column_transformer_transform(data: pd.DataFrame, **kwds):
    if data is None:
        return None
    else:
        column_transfomer = _get_column_transformer(**kwds)
        column_transfomer.set_output(transform="pandas")

        data = column_transfomer.transform(data)
        return data


def column_transformer_fit_transform(
    data: pd.DataFrame, dump_to_file: str = None, **kwds
):
    data = column_transformer_fit(data, dump_to_file, **kwds)
    data = column_transformer_transform(data, **kwds)
    return data


def add_date_offset_to_column(data, column, **kwds):
    if data is not None:
        data[column] += pd.offsets.DateOffset(**kwds)
    return data


def apply_transformations(data, transformations, quiet=False):
    __LOGGER__.disabled = quiet
    it = tqdm(transformations, disable=quiet, leave=False)
    __LOGGER__.debug("applying transformations")
    __LOGGER__.debug(transformations)
    with logging_redirect_tqdm():
        for kwds in it:
            desc = kwds.pop("description", kwds["id"])
            it.set_description(desc)
            data = _apply_transformation(
                data=data,
                quiet=quiet,
                **kwds,
            )
    return data
