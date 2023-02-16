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
