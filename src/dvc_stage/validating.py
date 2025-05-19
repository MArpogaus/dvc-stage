# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : validating.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 14:05:05 (Marcel Arpogaus)
# changed : 2025-05-19 11:12:16 (Marcel Arpogaus)


# %% Description ###############################################################
"""validating module."""

# %% imports ###################################################################
import inspect
import logging
from typing import Any, Dict, List, Union

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
    import_from: str = None,
    reduction: str = "any",
    expected: bool = True,
    include: List[str] = [],
    exclude: List[str] = [],
    pass_key_to_fn: bool = False,
    **kwds: Dict[str, Any],
) -> None:
    """Apply a validation function to a given data.

    Parameters
    ----------
    data : Union[pd.DataFrame, Dict[str, pd.DataFrame]]
        The data to be validated. It can be a DataFrame or a dictionary of DataFrames.
    id : str
        The identifier for the validation function to be applied.
        If 'custom', import_from is used as the function name.
    import_from : str, optional
        The module path of the custom validation function to be imported.
    reduction : str
        The method used to reduce the boolean result of the validation function.
        It can be:
          - 'any': the data will be considered valid if at least one of the rows or
            columns is valid.
          - 'all': the data will be considered valid only if all rows or
            columns are valid.
          - 'none': the data will not be reduced and the validation output will be
            returned in full.
    expected : bool
        The expected output of the validation.
    include : List[str]
        List of keys to include in the validation. If empty, all keys will be included.
    exclude : List[str]
        List of keys to exclude from the validation.
    pass_key_to_fn : bool
        Flag to indicate if the key should be passed to the validation function.
    kwds : Dict[str, Any]
        Additional keyword arguments to be passed to the validation function.

    Raises
    ------
    ValueError
        If the validation function with the given identifier is not found.
    AssertionError
        If the validation output does not match the expected output.

    """
    if isinstance(data, dict):
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
            f"Validation '{id}' with reduction method '{reduction}'"
            f"evaluated to: {reduced}\n"
            f"Expected: {expected}"
        )


# %% public functions ##########################################################
def validate_pandera_schema(
    data: pd.DataFrame, schema: Union[dict, str], **kwargs: Dict[str, Any]
) -> bool:
    """Validate a Pandas DataFrame `data` against a Pandera schema.

    Parameters
    ----------
    data : pandas.DataFrame
        Pandas DataFrame to be validated.
    schema : Union[dict, str]
        Schema to validate against.
        Can be specified as a dictionary with keys "import_from", "from_yaml",
        "from_json", or a string that specifies a file path to a serialized
        Pandera schema object.
    kwargs : Dict[str, Any]
        Optional keyword arguments to be passed to the Pandera schema function.

    Returns
    -------
    bool
        Returns True if the DataFrame validates against the schema.

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


def apply_validations(data: any, validations: List[dict]) -> None:
    """Apply validations to input data. Entrypoint for validation substage.

    Parameters
    ----------
    data : pandas.DataFrame or dict of pandas.DataFrame
        Input data.
    validations : List[dict]
        List of dictionaries containing validation parameters.

    """
    __LOGGER__.debug("applying validations")
    __LOGGER__.debug(validations)
    it = tqdm(validations, leave=False)
    with logging_redirect_tqdm():
        for kwds in it:
            it.set_description(kwds.pop("description", kwds["id"]))
            _apply_validation(data=data, **kwds)
