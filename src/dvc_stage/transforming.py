# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : transforming.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-24 14:40:39 (Marcel Arpogaus)
# changed : 2023-03-02 10:08:36 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
"""transforming module."""

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
    """Split data along date time axis.

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
    """Split data on a random set of ids.

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
    """Create an instance of the specified transformer class.

    :param transformer_class_name: The name of the transformer class.
    (function in python module, "drop" or "passthrough")
    :type transformer_class_name: str
    :param kwds: Optional keyword arguments to pass to the transformer
    class constructor.
    :returns: An instance of the specified transformer class.
    """
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
    """Build a Scikit-Learn ColumnTransformer from a list of dictionaries.

    :param transformers: list of transformer dictionaries.
    Each dictionary must contain a "class_name" key with the name
    of the transformer class, and a "columns" key with a list of column names to
    apply the transformer to.
    :type transformers: List[dict]
    :param remainder: how to handle columns that were not specified in the
    transformers, defaults to "drop"
    :type remainder: str, optional
    :param kwds: additional keyword arguments to pass to ColumnTransformer
    initialization
    :type kwds: dict
    :return: initialized ColumnTransformer object
    :rtype: object
    """
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
    """Return a callable function that transforms a pandas dataframe.

    :param data: Pandas DataFrame to be transformed
    :type data: Union[pd.DataFrame, None]
    :param id: Identifier for the transformation to be applied to the data.
    :type id: str
    :param import_from: When id="custom", it is the path to the
    python function to be imported.
    :type import_from: Optional[str]
    :return: A callable function that transforms a pandas dataframe.
    :rtype: Callable[..., Union[pd.DataFrame, None]]
    """
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
    """Apply transformation `id` to `data`.

    :param data: Input data to transform. Can be a single DataFrame or a
    dictionary of DataFrames.
    :type data: Union[pd.DataFrame, Dict[str, pd.DataFrame]]
    :param id: Transformation identifier. If it is 'combine' it will perform a
    combine operation.
    :type id: List[str]
    :param import_from: String representing the import path of a module
    containing a custom transformation function.
    :type import_from: Optional[str]
    :param exclude: List of keys to exclude from transformation.
    :type exclude: Optional[List[str]]
    :param include: List of keys to include in the transformation.
    :type include: Optional[List[str]]
    :param quiet: Flag to disable logger output.
    :type quiet: Optional[bool]
    :param pass_key_to_fn: Flag to pass the key value to the
    custom transformation function.
    :type pass_key_to_fn: Optional[bool]
    :param kwds: Optional keyword arguments to pass to the
    transformation function.
    :type kwds: Any
    :return: The transformed input data.
    :rtype: Union[Dict[str, Any], Any]
    """
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
        try:
            return fn(data, **kwds)
        except Exception as e:
            __LOGGER__.exception(
                f"Exception during execution of transformation with id {id}."
            )
            __LOGGER__.critical(str(locals()), stack_info=True)
            raise e


# PUBLIC FUNCTIONS ############################################################
def split(
    data: pd.DataFrame, by: str, left_split_key: str, right_split_key: str, **kwds
) -> Dict[str, pd.DataFrame]:
    """Split data along index.

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
    """Concatenate multiple DataFrames.

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
    """Fit the data to the input.

    :param data: Input data to fit the ColumnTransformer.
    :type data: pandas.DataFrame
    :param dump_to_file: Filepath to write fitted object to.
    :type dump_to_file: str, optional (default=None)
    :param **kwds:
        Other keyword arguments to be passed to the `_get_column_transformer`
        function.

    :return: pandas DataFrame
        The input data unchanged.
    """
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
    """Apply the column transformer to the input data.

    :param data: Input data to transform.
    :type data: pd.DataFrame
    :param **kwds: Additional keyword arguments to pass to the
    column transformer.
    :type **kwds: dict
    :return: Transformed data.
    :rtype: pd.DataFrame
    """
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
    """Fits and transform the input data.

    This function combines ..._fit and ..._transform.

    :param data: input data to be transformed.
    :type data: pd.DataFrame
    :param dump_to_file: if specified, saves the fitted column transformer to
    a file with the given name.
    :type dump_to_file: str
    :param kwds: keyword arguments to be passed to the column transformer.
    :type kwds: dict
    :return: the transformed data.
    :rtype: pd.DataFrame
    """
    data = column_transformer_fit(data, dump_to_file, **kwds)
    data = column_transformer_transform(data, **kwds)
    return data


def add_date_offset_to_column(data, column, **kwds):
    """Add a date offset to a date column in a pandas DataFrame.

    :param data: The input pandas DataFrame.
    :type data: pandas.DataFrame
    :param column: The name of the date column to which the offset
    will be applied.
    :type column: str
    :param **kwds: Additional arguments to be passed to pandas
    pd.offsets.DateOffset.
    :type **kwds: Any
    :return: The pandas DataFrame with the offset applied to the specified
    date column.
    :rtype: pandas.DataFrame
    """
    if data is not None:
        data[column] += pd.offsets.DateOffset(**kwds)
    return data


def apply_transformations(data, transformations, quiet=False):
    """Apply a list of transformations to a DataFrame or dict of DataFrames.

    The main entrypoint for transformations substage.

    :param data: The data to apply transformations to. Can be a pandas
    DataFrame or a dictionary of pandas DataFrames.
    :type data: Union[pd.DataFrame, Dict[str, pd.DataFrame]]
    :param transformations: A list of transformation dictionaries, each
    specifying an individual transformation to apply.
    :type transformations: List[Dict[str, Any]]
    :param quiet: Whether to suppress the progress bar and logging output.
    Default is False.
    :type quiet: bool
    :return: The transformed data.
    :rtype: Union[pd.DataFrame, Dict[str, pd.DataFrame]]
    """
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
