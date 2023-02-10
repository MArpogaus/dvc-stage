# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-10 15:38:43 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import logging
import os

from tqdm import tqdm


# PUBLIC FUNCTIONS ############################################################
def write_data(data, format, path, **kwds):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    if isinstance(data, list):
        logging.debug("data is list")
        for i, d in tqdm(enumerate(data)):
            write_data(
                format=format,
                data=d,
                path=path.format(item=i),
            )
    elif isinstance(data, dict):
        logging.debug("arg is dict")
        for k, v in tqdm(data.items()):
            write_data(
                format=format,
                data=v,
                path=path.format(key=k),
            )
    else:
        fn = getattr(data, "to_" + format)
        logging.debug(f"saving data to {path} as {format}")
        fn(path, **kwds)


def get_outs(data, path, **kwds):
    outs = []

    if isinstance(data, list):
        logging.debug("data is list")
        for i, d in enumerate(data):
            outs.append(path.format(item=i))
    elif isinstance(data, dict):
        logging.debug("arg is dict")
        for k, v in data.items():
            outs.append(path.format(key=k))
    else:
        logging.debug(f"path: {path}")
        outs.insert(path)

    return list(sorted(outs))
