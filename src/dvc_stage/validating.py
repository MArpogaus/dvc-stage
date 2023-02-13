# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : validating.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-24 14:40:56 (Marcel Arpogaus)
# changed : 2023-02-13 15:00:44 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import importlib
import logging

import numpy as np
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _get_validation(id, data, import_from):
    if id == "custom":
        module_name, function_name = import_from.rsplit(".", 1)
        fn = getattr(importlib.import_module(module_name), function_name)
    elif hasattr(data, id):
        fn = lambda _, **kwds: getattr(data, id)(**kwds)  # noqa E731
    else:
        raise ValueError(f'validation function "{id}" not found')
    return fn


def _apply_validation(
    result, id, import_from=None, reduction="any", expected=True, **kwds
):
    if isinstance(result, dict):
        __LOGGER__.debug("arg is dict")
        for k, v in tqdm(result.items()):
            __LOGGER__.debug(f"validating {k}")
            _apply_validation(
                result=v,
                id=id,
                import_from=import_from,
                **kwds,
            )
    else:
        __LOGGER__.debug(f"applying validation: {id}")
        fn = _get_validation(id, result, import_from)

        result = fn(result, **kwds)
        if reduction == "any":
            reduced = np.any(result)
        elif reduction == "all":
            reduced = np.all(result)
        elif reduction == "none":
            reduced = result
        else:
            raise ValueError(
                f"reduction method {reduction} unsupported."
                "can either be 'any', 'all' or 'none'."
            )

        assert reduced != expected, (
            f"Validation '{id}' with reduction method '{reduction}'"
            f"evaluated to: {reduced}\n"
            f"Expected: {expected}"
        )


# PUBLIC FUNCTIONS ############################################################
def apply_validations(data, validations):
    __LOGGER__.debug("applying validations")
    __LOGGER__.debug(validations)
    it = tqdm(validations)
    with logging_redirect_tqdm():
        for kwds in it:
            it.set_description(kwds.pop("description", kwds["id"]))
            _apply_validation(
                result=data,
                **kwds,
            )
