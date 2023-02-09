# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : validating.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-24 14:40:56 (Marcel Arpogaus)
# changed : 2023-02-09 16:44:29 (Marcel Arpogaus)
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

from .common import CUSTOM_MODULE_PREFIX


# PRIVATE FUNCTIONS ###########################################################
def _get_validation(data, name):
    if name.startswith(CUSTOM_MODULE_PREFIX):
        module_name, function_name = name.replace(CUSTOM_MODULE_PREFIX, "").rsplit(
            ".", 1
        )
        fn = getattr(importlib.import_module(module_name), function_name)
    elif hasattr(data, name):
        fn = lambda _, **kwds: getattr(data, name)(**kwds)  # noqa E731
    else:
        raise ValueError(f'validation function "{name}" not found')
    return fn


def _apply_validation(name, data, reduction="any", expected=True, **kwds):
    if isinstance(data, list):
        logging.debug("arg is list")
        for d in tqdm(data):
            _apply_validation(
                name=name,
                data=d,
                **kwds,
            )
    if isinstance(data, dict):
        logging.debug("arg is dict")
        for k, v in tqdm(data.items()):
            logging.debug(f"validating {k}")
            _apply_validation(
                name=name,
                data=v,
                **kwds,
            )
    else:
        logging.debug(f"applying {name}")
        fn = _get_validation(data, name)

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

        assert reduced != expected, (
            f"Validation '{name}' with reduction method '{reduction}'"
            f"evaluated to: {reduced}\n"
            f"Expected: {expected}"
        )


# PUBLIC FUNCTIONS ############################################################
def apply_validations(data, validations):
    logging.debug("applying validations")
    logging.debug(validations)
    it = tqdm(validations.items())
    with logging_redirect_tqdm():
        for name, kwds in it:
            it.set_description(name)
            _apply_validation(
                name=name,
                data=data,
                **kwds,
            )
