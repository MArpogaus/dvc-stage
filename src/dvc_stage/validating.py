# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : validating.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-24 14:40:56 (Marcel Arpogaus)
# changed : 2022-12-13 13:37:16 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import logging

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


# PRIVATE FUNCTIONS ###########################################################
def _no_nans(data):
    assert not data.isnull().any().any(), "Data contains NaNs"


def _apply_validation(name, data, **kwds):
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
        fn = VALIDATION_FUNCTIONS[name]
        fn(data, **kwds)


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


# GLOBAL VARIABLES ############################################################
VALIDATION_FUNCTIONS = {"no_nans": _no_nans}
