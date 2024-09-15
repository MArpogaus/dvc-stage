# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : transforming.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 13:54:07 (Marcel Arpogaus)
# changed : 2024-09-15 14:37:13 (Marcel Arpogaus)

# %% Description ###############################################################
"""Module defining common transformations."""

# %% imports ###################################################################
import importlib
import logging
import os
import pickle
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.utils import import_from_string, key_is_skipped

# %% globals ###################################################################
__COLUMN_TRANSFORMER_CACHE__ = {}
__LOGGER__ = logging.getLogger(__name__)


# %% private functions #########################################################
def _date_time_split(
    data: pd.DataFrame, size: float, freq: str, date_time_col: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split data along date time axis.

    NOTE: Only tested for Monthly splits so far

    Parameters
    ----------
    data : pd.DataFrame
        Data to split.
    size : float
        Amount of time steps.
    freq : str
        Frequency to split on.
    date_time_col : str
        Column containing the date time index.

    Returns
    -------
    tuple
        Tuple containing left and right split data.

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

    Parameters
    ----------
    data : pd.DataFrame
        Data to split.
    size : float
        Amount of random ids in the left split.
    seed : int
        Seed used for id shuffling.
    id_col : str
        Column containing id information.

    Returns
    -------
    tuple
        Tuple containing left and right split data.

    """
    np.random.seed(seed)
    ids = list(sorted(data[id_col].unique()))
    np.random.shuffle(ids)
    ids = ids[: int(size * len(ids))]
    mask = data[id_col].isin(ids)
    return data[mask], data[~mask]


def _initialize_sklearn_transformer(transformer_class_name: str, **kwds: Any) -> Any:
    """Create an instance of the specified transformer class.

    Parameters
    ----------
    transformer_class_name : str
        The name of the transformer class, "drop" or "passthrough".
    kwds : Any
        Optional keyword arguments to pass to the transformer class constructor.

    Returns
    -------
    object
        An instance of the specified transformer class.

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


def _get_column_transformer(
    transformers: List[Dict[str, Any]], remainder: str = "drop", **kwds: Any
) -> Any:
    """Build a Scikit-Learn ColumnTransformer from a list of dictionaries.

    Parameters
    ----------
    transformers : list
        List of transformer dictionaries.
        Each dictionary must contain a "class_name" key with the name of the transformer
        class, and a "columns" key with a list of columns to apply the transformer to.
    remainder : str, optional
        How to handle columns that were not specified in the transformers.
        Default: "drop"
    kwds : dict
        Additional keyword arguments to pass to ColumnTransformer initialization.

    Returns
    -------
    object
        Initialized ColumnTransformer object.

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


def _get_transformation(
    data: Optional[pd.DataFrame], id: str, import_from: Optional[str]
) -> Callable[..., Union[pd.DataFrame, None]]:
    """Return a callable function that transforms a pandas dataframe.

    Parameters
    ----------
    data : pd.DataFrame, optional
        Pandas DataFrame to be transformed.
    id : str
        Identifier for the transformation to be applied to the data.
    import_from : str, optional
        When id="custom", it is the path to the python function to be imported.

    Returns
    -------
    callable
        A callable function that transforms a pandas dataframe.

    """
    if id == "custom":
        fn = import_from_string(import_from)
    elif id in globals().keys():
        fn = globals()[id]
    elif hasattr(data, id):
        fn = lambda _, **kwds: getattr(data, id)(**kwds)  # noqa: E731
    elif data is None and hasattr(pd.DataFrame, id):
        fn = lambda _, **__: None  # noqa: E731
    else:
        raise ValueError(f'transformation function "{id}" not found')
    return fn


def _apply_transformation(
    data: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
    id: str,
    import_from: Optional[str] = None,
    exclude: Optional[List[str]] = [],
    include: Optional[List[str]] = [],
    quiet: bool = False,
    pass_key_to_fn: bool = False,
    **kwds: Any,
) -> Union[Dict[str, Any], Any]:
    """Apply transformation `id` to `data`.

    Parameters
    ----------
    data : pd.DataFrame or dict
        Input data to transform. Can be a single DataFrame or a dict of DataFrames.
    id : str
        Identifier of transformation to apply, passed to `_get_transformation`.
    import_from : str, optional
        String representing the import path of a custom transformation function.
    exclude : list, optional
        List of keys to exclude from transformation.
    include : list, optional
        List of keys to include in the transformation.
    quiet : bool, optional
        Flag to disable logger output.
    pass_key_to_fn : bool, optional
        Flag to pass the key value to the custom transformation function.
    kwds : Any
        Additional keyword arguments to pass to the transformation function.

    Returns
    -------
    dict or any
        The transformed input data.

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


# %% public functions ##########################################################
def split(
    data: pd.DataFrame, by: str, left_split_key: str, right_split_key: str, **kwds: Any
) -> Dict[str, Optional[pd.DataFrame]]:
    """Split data along index.

    Parameters
    ----------
    data : pd.DataFrame
        Data to split.
    by : str
        Type of split.
    left_split_key : str
        Key for left split
    right_split_key : str
        Key for right split
    kwds : Any
        Additional keyword arguments to pass to the splitting function.

    Returns
    -------
    dict
        Dictionary containing left and right split data.

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
) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
    """Concatenate multiple DataFrames.

    Parameters
    ----------
    data : dict
        Dictionary with data frames to concatenate.
    include : list
        Keys to include.
    exclude : list
        Keys to exclude.
    new_key : str
        New key for concatenated data.

    Returns
    -------
    dict or pd.DataFrame
        Dictionary with combined data or combined DataFrame.

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


def column_transformer_fit(
    data: pd.DataFrame, dump_to_file: Optional[str] = None, **kwds: Any
) -> Optional[pd.DataFrame]:
    """Fit the data to the input.

    Parameters
    ----------
    data : pd.DataFrame
        Input data to fit the ColumnTransformer.
    dump_to_file : str, optional
        Filepath to write fitted object to.
    kwds : dict
        Additional keyword arguments to be passed to `_get_column_transformer`.

    Returns
    -------
    pd.DataFrame
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


def column_transformer_transform(
    data: pd.DataFrame, **kwds: Any
) -> Optional[pd.DataFrame]:
    """Apply the column transformer to the input data.

    Parameters
    ----------
    data : pd.DataFrame
        Input data to transform.
    kwds : dict
        Additional keyword arguments to pass to the column transformer.

    Returns
    -------
    pd.DataFrame
        Transformed data.

    """
    if data is None:
        return None
    else:
        column_transfomer = _get_column_transformer(**kwds)
        column_transfomer.set_output(transform="pandas")

        data = column_transfomer.transform(data)
        return data


def column_transformer_fit_transform(
    data: pd.DataFrame, dump_to_file: Optional[str] = None, **kwds: Any
) -> Optional[pd.DataFrame]:
    """Fits and transform the input data.

    This function combines ..._fit and ..._transform.

    Parameters
    ----------
    data : pd.DataFrame
        Input data to be transformed.
    dump_to_file : str, optional
        If specified, saves the fitted column transformer to a file with the given name.
    kwds : dict
        Keyword arguments to be passed to the column transformer.

    Returns
    -------
    pd.DataFrame
        The transformed data.

    """
    data = column_transformer_fit(data, dump_to_file, **kwds)
    data = column_transformer_transform(data, **kwds)
    return data


def add_date_offset_to_column(
    data: pd.DataFrame, column: str, **kwds: Any
) -> Optional[pd.DataFrame]:
    """Add a date offset to a date column in a pandas DataFrame.

    Parameters
    ----------
    data : pd.DataFrame
        The input pandas DataFrame.
    column : str
        The name of the date column to which the offset will be applied.
    kwds : any
        Additional arguments to be passed to pandas pd.offsets.DateOffset.

    Returns
    -------
    pd.DataFrame
        The pandas DataFrame with the offset applied to the specified date column.

    """
    if data is not None:
        data[column] += pd.offsets.DateOffset(**kwds)
    return data


def apply_transformations(
    data: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
    transformations: List[Dict[str, Any]],
    quiet: bool = False,
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """Apply a list of transformations to a DataFrame or dict of DataFrames.

    The main entrypoint for transformations substage.

    Parameters
    ----------
    data : pd.DataFrame or dict
        The data to apply transformations to.
        Can be a DataFrame or a dict of DataFrames.
    transformations : list
        A list of transformation dictionaries, each specifying
        individual transformation to apply.
    quiet : bool, optional
        Whether to suppress the progress bar and logging output. Default is False.

    Returns
    -------
    pd.DataFrame or dict
        The transformed data.

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
