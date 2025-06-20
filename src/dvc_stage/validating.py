# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : validating.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 14:05:05 (Marcel Arpogaus)
# changed : 2025-06-20 14:55:53 (Marcel Arpogaus)


# %% Description ###############################################################
"""validating module."""

# %% imports ###################################################################
from __future__ import annotations

import inspect
import logging

import numpy as np
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.utils import import_from_string, key_is_skipped

# %% globals ###################################################################
__LOGGER__ = logging.getLogger(__name__)


# %% global functions ##########################################################
def _get_validation(id: str, data: any, import_from: str) -> callable:
    """Return the validation function with the given ID.

    Parameters
    ----------
    id : str
        ID of the validation function to get.
    data : Any
        Data source to be validated.
    import_from : str
        Import path to the custom validation function (if ``id="custom"``).

    Returns
    -------
    callable
        The validation function.

    Raises
    ------
    ValueError
        If the validation function with the given ID is not found.

    """
    if id == "custom":
        fn = import_from_string(import_from)
    elif hasattr(data, id):
        fn = lambda _, **kwds: getattr(data, id)(**kwds)  # noqa E731
    elif id in globals().keys():
        fn = globals()[id]
    else:
        raise ValueError(f'validation function "{id}" not found')
    return fn


def _apply_validation(
    data: any,
    id: str,
    import_from: str | None = None,
    reduction: str = "any",
    expected: bool = True,
    include: list[str] = [],
    exclude: list[str] = [],
    pass_key_to_fn: bool = False,
    pass_dict_to_fn: bool = False,
    **kwds: dict[str, any],
) -> None:
    """Apply a validation function to given data.

    Parameters
    ----------
    data : pd.DataFrame | dict[str, pd.DataFrame]
        The data to be validated. Can be a DataFrame or a dictionary of DataFrames.
    id : str
        The identifier for the validation function to be applied.
        If 'custom', import_from is used as the function name.
    import_from : str | None, optional
        The module path of the custom validation function to be imported.
        Default is None.
    reduction : str, optional
        The method used to reduce the boolean result of the validation function.
        Can be:
        - 'any': data is valid if at least one row/column is valid
        - 'all': data is valid only if all rows/columns are valid
        - 'none': data is not reduced and validation output is returned in full
        Default is "any".
    expected : bool, optional
        The expected output of the validation. Default is True.
    include : list[str], optional
        List of keys to include in validation. If empty, all keys are included.
        Default is None.
    exclude : list[str], optional
        List of keys to exclude from validation. Default is None.
    pass_key_to_fn : bool, optional
        If True, pass the key value to the custom validation function. Default is False.
    pass_dict_to_fn : bool, optional
        If True, pass the raw data dict to the validation function. Default is False.
    **kwds : dict[str, any]
        Additional keyword arguments passed to the validation function.

    Raises
    ------
    ValueError
        If the validation function with the given identifier is not found
        or if the reduction method is unsupported.
    AssertionError
        If the validation output does not match the expected output.

    """
    if isinstance(data, dict) and not pass_dict_to_fn:
        __LOGGER__.debug("arg is dict")
        it = tqdm(data.items(), leave=False)
        for key, df in it:
            description = f"validating df with key '{key}'"
            __LOGGER__.debug(description)
            it.set_description(description)
            if not key_is_skipped(key, include, exclude):
                if pass_key_to_fn:
                    kwds.update({"key": key})
                _apply_validation(
                    data=df,
                    id=id,
                    import_from=import_from,
                    reduction=reduction,
                    expected=expected,
                    include=include,
                    exclude=exclude,
                    **kwds,
                )
    else:
        __LOGGER__.debug(f"applying validation: {id}")
        fn = _get_validation(id, data, import_from)

        if pass_dict_to_fn:
            kwds["include"] = include
            kwds["exclude"] = exclude

        try:
            data = fn(data, **kwds)
        except Exception as e:
            __LOGGER__.exception(
                f"Exception during execution of validation with id {id}."
            )
            __LOGGER__.critical(str(locals()), stack_info=True)
            raise e

        if reduction == "any":
            reduced = np.any(data)
        elif reduction == "all":
            reduced = np.all(data)
        elif reduction == "none":
            reduced = data
        else:
            raise ValueError(
                f"reduction method {reduction} unsupported."
                "can either be 'any', 'all' or 'none'."
            )

        assert reduced == expected, (
            f"Validation '{id}' with reduction method '{reduction}' "
            f"evaluated to: {reduced}\n"
            f"Expected: {expected}"
        )


# %% public functions ##########################################################
def validate_pandera_schema(
    data: pd.DataFrame, schema: dict | str, **kwargs: dict[str, any]
) -> bool:
    """Validate a Pandas DataFrame against a Pandera schema.

    Parameters
    ----------
    data : pd.DataFrame
        Pandas DataFrame to be validated.
    schema : dict | str
        Schema to validate against. Can be specified as a dictionary with
        keys "import_from", "from_yaml", "from_json", or a string that specifies
        a file path to a serialized Pandera schema object.
    **kwargs : dict[str, any]
        Optional keyword arguments passed to the Pandera schema function.

    Returns
    -------
    bool
        True if the DataFrame validates against the schema.

    Raises
    ------
    ValueError
        If the schema is of an invalid type or if the schema cannot be
        deserialized from the provided dictionary or file.

    """
    import pandera as pa

    if isinstance(schema, dict):
        if "import_from" in schema.keys():
            import_from = schema["import_from"]
            schema = import_from_string(import_from)
            if not isinstance(schema, pa.DataFrameSchema):
                if callable(schema):
                    sig = inspect.signature(schema)
                    if len(sig.parameters):
                        schema = schema(**kwargs)
                    else:
                        schema = schema()
                else:
                    raise ValueError(
                        f"Schema imported from {import_from} has invalid type: {type(schema)}"  # noqa E501
                    )
        elif "from_yaml" in schema.keys():
            schema = pa.DataFrameSchema.from_yaml(schema["from_yaml"])
        elif "from_json" in schema.keys():
            schema = pa.DataFrameSchema.from_json(schema["from_json"])
        else:
            from pandera.io import deserialize_schema

            schema = deserialize_schema(schema)
    else:
        raise ValueError(
            f"Schema has invalid type '{type(schema)}', dictionary expected."
        )

    schema.validate(data)
    return True


def apply_validations(
    data: any,
    validations: list[dict],
    item: str | None = None,
) -> None:
    """Apply validations to input data. Entrypoint for validation substage.

    Parameters
    ----------
    data : pd.DataFrame | dict[str, pd.DataFrame]
        Input data.
    validations : list[dict]
        List of dictionaries containing validation parameters.
    item : str | None, optional
        Item identifier for foreach stages. Default is None.

    """
    __LOGGER__.debug("applying validations")
    __LOGGER__.debug(validations)
    it = tqdm(validations, leave=False)
    with logging_redirect_tqdm():
        for kwds in it:
            it.set_description(kwds.pop("description", kwds["id"]))
            if kwds.pop("pass_item_to_fn", False):
                kwds["item"] = item
            _apply_validation(data=data, **kwds)
