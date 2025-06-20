# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : transforming.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 13:54:07 (Marcel Arpogaus)
# changed : 2025-06-20 14:58:09 (Marcel Arpogaus)

# %% Description ###############################################################
"""Module defining common transformations."""

# %% imports ###################################################################
from __future__ import annotations

import importlib
import logging
import os
import pickle

import numpy as np
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.utils import import_from_string, key_is_skipped, parse_path

# %% globals ###################################################################
__COLUMN_TRANSFORMER_CACHE__ = {}
__LOGGER__ = logging.getLogger(__name__)


# %% private functions #########################################################
def _date_time_split(
    data: pd.DataFrame, size: float, freq: str, date_time_col: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split data along date time axis.

    Notes
    -----
    Only tested for Monthly splits so far.

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
    tuple[pd.DataFrame, pd.DataFrame]
        Tuple containing left and right split data.

    """
    start_point = data[date_time_col].dt.date.min()
    end_date = data[date_time_col].dt.date.max()

    data.set_index(date_time_col, inplace=True)

    # Reserve some data for testing
    period_range = pd.period_range(start_point, end_date, freq=freq)
    periods = len(period_range)
    split_point = int(np.round(size * periods))
    left_periods = period_range[:split_point]
    right_periods = period_range[split_point:]
    __LOGGER__.debug(f"left split from {left_periods.min()} till {left_periods.max()}")
    __LOGGER__.debug(
        f"right split from {right_periods.min()} till {right_periods.max()}"
    )

    left_data = data.loc[: str(left_periods.max())].reset_index()
    right_data = data.loc[str(right_periods.min()) :].reset_index()

    return left_data, right_data


def _id_split(
    data: pd.DataFrame, size: float, seed: int, id_col: str | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split data on a random set of ids.

    Parameters
    ----------
    data : pd.DataFrame
        Data to split.
    size : float
        Amount of random ids in the left split.
    seed : int
        Seed used for id shuffling.
    id_col : str | None, optional
        Column containing id information. Default is None.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        Tuple containing left and right split data.

    """
    np.random.seed(seed)
    if id_col:
        ids = data[id_col]
    else:
        ids = data.index
    unique_ids = list(sorted(ids.unique()))
    np.random.shuffle(unique_ids)
    selected_ids = unique_ids[: int(size * len(unique_ids))]
    mask = ids.isin(selected_ids)
    return data[mask], data[~mask]


def _initialize_sklearn_transformer(transformer_class_name: str, **kwds: any) -> any:
    """Create an instance of the specified transformer class.

    Parameters
    ----------
    transformer_class_name : str
        The name of the transformer class, "drop" or "passthrough".
    **kwds : any
        Optional keyword arguments to pass to the transformer class constructor.

    Returns
    -------
    any
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
    transformers: list[dict[str, any]], remainder: str = "drop", **kwds: any
) -> any:
    """Build a Scikit-Learn ColumnTransformer from a list of dictionaries.

    Parameters
    ----------
    transformers : list[dict[str, any]]
        List of transformer dictionaries.
        Each dictionary must contain a "class_name" key with the name of the transformer
        class, and a "columns" key with a list of columns to apply the transformer to.
    remainder : str, optional
        How to handle columns that were not specified in the transformers.
        Default is "drop".
    **kwds : any
        Additional keyword arguments to pass to ColumnTransformer initialization.

    Returns
    -------
    any
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
    data: pd.DataFrame | None, id: str, import_from: str | None
) -> callable:
    """Return a callable function that transforms a pandas dataframe.

    Parameters
    ----------
    data : pd.DataFrame | None
        Pandas DataFrame to be transformed.
    id : str
        Identifier for the transformation to be applied to the data.
    import_from : str | None
        When id="custom", it is the path to the python function to be imported.

    Returns
    -------
    callable
        A callable function that transforms a pandas dataframe.

    Raises
    ------
    ValueError
        If the transformation function is not found.

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
    data: pd.DataFrame | dict[str, pd.DataFrame],
    id: str,
    import_from: str | None = None,
    exclude: list[str] = [],
    include: list[str] = [],
    quiet: bool = False,
    pass_key_to_fn: bool = False,
    pass_dict_to_fn: bool = False,
    **kwds: any,
) -> dict[str, pd.DataFrame] | pd.DataFrame:
    """Apply transformation to data.

    Parameters
    ----------
    data : pd.DataFrame | dict[str, pd.DataFrame]
        Input data to transform. Can be a single DataFrame or a dict of DataFrames.
    id : str
        Identifier of transformation to apply, passed to `_get_transformation`.
    import_from : str | None, optional
        String representing the import path of a custom transformation function.
        Default is None.
    exclude : list[str], optional
        List of keys to exclude from transformation. Default is None.
    include : list[str], optional
        List of keys to include in the transformation. Default is None.
    quiet : bool, optional
        If True, disable logger output. Default is False.
    pass_key_to_fn : bool, optional
        If True, pass the key value to the custom transformation function.
        Default is False.
    pass_dict_to_fn : bool, optional
        If True, pass the raw data dict to the transformation function.
        Default is False.
    **kwds : any
        Additional keyword arguments to pass to the transformation function.

    Returns
    -------
    dict[str, any] | any
        The transformed input data.

    Raises
    ------
    Exception
        If an exception occurs during transformation execution.

    """
    __LOGGER__.disabled = quiet
    # Always pass dict to combine function
    pass_dict_to_fn = id == "combine"
    if isinstance(data, dict) and not pass_dict_to_fn:
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
    else:
        __LOGGER__.debug(f"applying transformation: {id}")
        fn = _get_transformation(data, id, import_from)

        if pass_dict_to_fn:
            kwds["include"] = include
            kwds["exclude"] = exclude
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
    data: pd.DataFrame, by: str, left_split_key: str, right_split_key: str, **kwds: any
) -> dict[str, pd.DataFrame | None]:
    """Split data along index.

    Parameters
    ----------
    data : pd.DataFrame
        Data to split.
    by : str
        Type of split.
    left_split_key : str
        Key for left split.
    right_split_key : str
        Key for right split.
    **kwds : any
        Additional keyword arguments to pass to the splitting function.

    Returns
    -------
    dict[str, pd.DataFrame | None]
        Dictionary containing left and right split data.

    Raises
    ------
    ValueError
        If an invalid choice for split is provided.

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
    data: dict[str, pd.DataFrame],
    include: list[str],
    exclude: list[str],
    new_key: str = "combined",
) -> pd.DataFrame | None:
    """Concatenate multiple DataFrames.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        Dictionary with data frames to concatenate.
    include : list[str]
        Keys to include.
    exclude : list[str]
        Keys to exclude.
    new_key : str, optional
        New key for concatenated data. Default is "combined".

    Returns
    -------
    pd.DataFrame | None
        The combined DataFrame.

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
    data: pd.DataFrame,
    dump_to_file: str | None = None,
    item: str | None = None,
    **kwds: any,
) -> pd.DataFrame | None:
    """Fit the data to the input.

    Parameters
    ----------
    data : pd.DataFrame
        Input data to fit the ColumnTransformer.
    dump_to_file : str | None, optional
        Filepath to write fitted object to. Default is None.
    item : str | None, optional
        Item identifier for foreach stages. Default is None.
    **kwds : any
        Additional keyword arguments passed to `_get_column_transformer`.

    Returns
    -------
    pd.DataFrame | None
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
            path = parse_path(dump_to_file, item=item)[0]
            with open(path, "wb+") as file:
                pickle.dump(column_transfomer, file)

        return data


def column_transformer_transform(
    data: pd.DataFrame, **kwds: any
) -> pd.DataFrame | None:
    """Apply the column transformer to the input data.

    Parameters
    ----------
    data : pd.DataFrame
        Input data to transform.
    **kwds : any
        Additional keyword arguments to pass to the column transformer.

    Returns
    -------
    pd.DataFrame | None
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
    data: pd.DataFrame, dump_to_file: str | None = None, **kwds: any
) -> pd.DataFrame | None:
    """Fit and transform the input data.

    This function combines column_transformer_fit and column_transformer_transform.

    Parameters
    ----------
    data : pd.DataFrame
        Input data to be transformed.
    dump_to_file : str | None, optional
        If specified, saves the fitted column transformer to a file with the given name.
        Default is None.
    **kwds : any
        Keyword arguments passed to the column transformer.

    Returns
    -------
    pd.DataFrame | None
        The transformed data.

    """
    data = column_transformer_fit(data, dump_to_file, **kwds)
    data = column_transformer_transform(data, **kwds)
    return data


def add_date_offset_to_column(
    data: pd.DataFrame,
    column: str,
    **kwds: any,
) -> pd.DataFrame | None:
    """Add a date offset to a date column in a pandas DataFrame.

    Parameters
    ----------
    data : pd.DataFrame
        The input pandas DataFrame.
    column : str
        The name of the date column to which the offset will be applied.
    **kwds : any
        Additional arguments passed to pandas pd.offsets.DateOffset.

    Returns
    -------
    pd.DataFrame | None
        The pandas DataFrame with the offset applied to the specified date column.

    """
    if data is not None:
        data[column] += pd.offsets.DateOffset(**kwds)
    return data


def apply_transformations(
    data: pd.DataFrame | dict[str, pd.DataFrame],
    transformations: list[dict[str, any]],
    quiet: bool = False,
    item: str | None = None,
) -> dict[str, pd.DataFrame] | pd.DataFrame:
    """Apply a list of transformations to a DataFrame or dict of DataFrames.

    The main entrypoint for transformations substage.

    Parameters
    ----------
    data : pd.DataFrame | dict[str, pd.DataFrame]
        The data to apply transformations to.
        Can be a DataFrame or a dict of DataFrames.
    transformations : list[dict[str, any]]
        A list of transformation dictionaries, each specifying
        individual transformation to apply.
    quiet : bool, optional
        Whether to suppress the progress bar and logging output. Default is False.
    item : str | None, optional
        Item identifier for foreach stages. Default is None.

    Returns
    -------
    pd.DataFrame | dict[str, pd.DataFrame]
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
            if kwds.pop("pass_item_to_fn", False):
                kwds["item"] = item
            data = _apply_transformation(
                data=data,
                quiet=quiet,
                **kwds,
            )
    return data
