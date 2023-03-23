# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-16 13:00:37 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import logging
import os

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.utils import import_from_string

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _get_writing_function(data, format, import_from):
    """
    Returns a writing function for a given data format.
    
    :param data: The data to be written.
    :type data: Any
    :param format: The format to write the data in.
    :type format: str
    :param import_from: The module path for the custom writing function (default: None).
    :type import_from: Optional[str]
    :return: The writing function.
    :rtype: Callable
    :raises ValueError: If the writing function for the given format is not found.
    """
    if format == "custom":
        fn = import_from_string(import_from)
    elif hasattr(data, "to_" + format):
        fn = lambda _, path: getattr(data, "to_" + format)(path)  # noqa E731
    else:
        raise ValueError(f'writing function for format "{format}" not found')
    return fn


# PUBLIC FUNCTIONS ############################################################
def write_data(data, format, path, import_from=None, **kwds):
    """
    Write data to a file. Main entrypoint for writing substage.

    :param data: The data to be written.
    :type data: Union[pandas.DataFrame, Dict[str, pandas.DataFrame]]
    :param format: The format of the output file.
    :type format: str
    :param path: The path to write the file to.
    :type path: str
    :param import_from: The module path of a custom writing function.
    :type import_from: Optional[str]
    :param kwds: Additional keyword arguments passed to the writing function.
    :type kwds: Any
    """
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    if isinstance(data, dict):
        __LOGGER__.debug("arg is dict")
        it = tqdm(data.items(), leave=False)
        with logging_redirect_tqdm():
            for k, v in it:
                formatted_path = path.format(key=k)
                __LOGGER__.debug(f"writing df with key {k} to '{formatted_path}'")
                it.set_description(f"writing df with key {k}")
                write_data(
                    format=format,
                    data=v,
                    path=formatted_path,
                )
    else:
        __LOGGER__.debug(f"saving data to {path} as {format}")
        fn = _get_writing_function(data, format, import_from)
        fn(data, path, **kwds)


def get_outs(data, path, **kwds):
    """
    Get list of output paths based on input data.

    :param data: Input data
    :type data: Union[List, Dict, pd.DataFrame]
    :param path: Output path template string
    :type path: str
    :param **kwds: Additional keyword arguments
    :type **kwds: dict
    :return: List of output paths
    :rtype: List[str]
    """
    outs = []

    if isinstance(data, list):
        __LOGGER__.debug("data is list")
        for i, d in enumerate(data):
            outs.append(path.format(item=i))
    elif isinstance(data, dict):
        __LOGGER__.debug("arg is dict")
        for k, v in data.items():
            outs.append(path.format(key=k))
    else:
        __LOGGER__.debug(f"path: {path}")
        outs.append(path)

    return list(sorted(outs))
