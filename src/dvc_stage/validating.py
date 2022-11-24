# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : validating.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-24 14:40:56 (Marcel Arpogaus)
# changed : 2021-03-26 11:48:25 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import logging

from tqdm import tqdm


# FUNCTION DEFINITIONS ########################################################
def no_nans(data):
    assert not data.isnull().any().any(), "Data contains NaNs"


def validate_data(name, data, custom_validation_functions, **kwds):
    if isinstance(data, list) and name != "combine":
        logging.debug("arg is list")
        for d in tqdm(data):
            validate_data(
                name=name,
                data=d,
                custom_validation_functions=custom_validation_functions,
                **kwds,
            )
    if isinstance(data, dict):
        logging.debug("arg is dict")
        for v in tqdm(data.values()):
            validate_data(
                name=name,
                data=v,
                custom_validation_functions=custom_validation_functions,
                **kwds,
            )
    else:
        logging.debug(f"applying {name}")
        fn = custom_validation_functions.get(name, VALIDATION_FUNCTIONS[name])
        fn(data, **kwds)


# GLOBAL VARIABLES ############################################################
VALIDATION_FUNCTIONS = {"no_nans": no_nans}
