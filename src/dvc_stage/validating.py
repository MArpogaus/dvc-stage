# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : validating.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-24 14:40:56 (Marcel Arpogaus)
# changed : 2023-02-16 13:01:59 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import logging

import numpy as np
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.utils import import_from_string, key_is_skipped

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _get_validation(id, data, import_from):
    """
    Returns the validation function with the given ID.

    :param id: ID of the validation function to get.
    :type id: str
    :param data: Data source to be validated.
    :type data: Any
    :param import_from: Import path to the custom validation function (if ``id="custom"``), defaults to None.
    :type import_from: Optional[str], optional
    :raises ValueError: If the validation function with the given ID is not found.
    :return: The validation function.
    :rtype: Callable
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
    data,
    id,
    import_from=None,
    reduction="any",
    expected=True,
    include=[],
    exclude=[],
    pass_key_to_fn=False,
    **kwds,
):
        """
    Apply a validation function to a given data. 
    
    :param data: The data to be validated. It can be a DataFrame or a dictionary of DataFrames.
    :type data: Union[pd.DataFrame, Dict[str, pd.DataFrame]]
    
    :param id: The identifier for the validation function to be applied. If 'custom', import_from is used as the function name.
    :type id: str
    
    :param import_from: The module path of the custom validation function to be imported.
    :type import_from: Optional[str]
    
    :param reduction: The method used to reduce the boolean result of the validation function. 
        It can be 'any', 'all' or 'none'. 
        If 'any', the data will be considered valid if at least one of the rows or columns is valid.
        If 'all', the data will be considered valid only if all rows or columns are valid.
        If 'none', the data will not be reduced and the validation output will be returned in full. 
    :type reduction: str
    
    :param expected: The expected output of the validation. 
    :type expected: bool
    
    :param include: List of keys to include in the validation. If empty, all keys will be included.
    :type include: List[str]
    
    :param exclude: List of keys to exclude from the validation.
    :type exclude: List[str]
    
    :param pass_key_to_fn: Flag to indicate if the key should be passed to the validation function.
    :type pass_key_to_fn: bool
    
    :param kwds: Additional keyword arguments to be passed to the validation function.
    
    :raises ValueError: If the validation function with the given identifier is not found.
    :raises AssertionError: If the validation output does not match the expected output.
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

        data = fn(data, **kwds)
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


# PUBLIC FUNCTIONS ############################################################
def validate_pandera_schema(data, schema, key=None):
    """
    Validate a Pandas DataFrame `data` against a Pandera schema.

    :param data: Pandas DataFrame to be validated.
    :type data: pandas.DataFrame
    :param schema: Schema to validate against. Can be specified as a dictionary with keys "import_from", "from_yaml", "from_json" or a string that specifies a file path to a serialized Pandera schema object.
    :type schema: Union[dict, str]
    :param key: Optional key to be passed to the Pandera schema function.
    :type key: Optional[str]
    :return: Returns True if the DataFrame validates against the schema.
    :rtype: bool
    :raises ValueError: If the schema is of an invalid type or if the schema cannot be deserialized from the provided dictionary or file.
    """
    import pandera as pa

    if isinstance(schema, dict):
        if "import_from" in schema.keys():
            import_from = schema["import_from"]
            schema = import_from_string(import_from)
            if not isinstance(schema, pa.DataFrameSchema):
                if callable(schema):
                    schema = schema(key)
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


def apply_validations(data, validations):
    """
    Apply validations to input data. Entrypoint for validation substage.

    :param data: Input data.
    :type data: pandas.DataFrame or dict of pandas.DataFrame

    :param validations: List of dictionaries containing validation parameters.
    :type validations: list
    """
    __LOGGER__.debug("applying validations")
    __LOGGER__.debug(validations)
    it = tqdm(validations, leave=False)
    with logging_redirect_tqdm():
        for kwds in it:
            it.set_description(kwds.pop("description", kwds["id"]))
            _apply_validation(
                data=data,
                **kwds,
            )
