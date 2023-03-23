# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-16 09:22:13 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import glob
import importlib
import logging
import re
from typing import Dict

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _parse_path(path, params) -> Dict:
    """
    Parses a path string and replaces ${PLACEHOLDERS}" with values from a dictionary.

    :param path: The path string to parse.
    :type path: str
    :param params: A dictionary of parameter values to replace placeholders.
    :type params: Dict[str, Any]
    :return: A tuple containing the parsed path string and a set of the matched parameter names.
    :rtype: Tuple[str, Set[str]]
    """
    pattern = re.compile(r"\${([a-z]+)}")  # noqa: W605
    matches = set(re.findall(pattern, path))
    for g in matches:
        path = path.replace("${" + g + "}", params[g])
    return path, matches


# PUBLIC FUNCTIONS ############################################################
def flatten_dict(d, parent_key="", sep="."):
    """
    Recursively flattens a nested dictionary into a single-level dictionary.

    :param d: The dictionary to flatten.
    :type d: dict
    :param parent_key: The parent key for the current level of the dictionary.
    :type parent_key: str
    :param sep: The separator to use between keys.
    :type sep: str
    :return: The flattened dictionary.
    :rtype: dict
    """
    items = []
    for k, v in d.items():
        new_key = sep.join((parent_key, k)) if parent_key else k
        if isinstance(v, dict) and len(v):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_deps(path, params):
def get_deps(path: Union[str, List[str]], params: Dict[str, Any]) -> Tuple[List[str], Set[str]]:
    """
    Get dependencies given a path pattern and parameter values.

    :param path: A string or list of strings representing file paths.
    :type path: Union[str, List[str]]
    :param params: A dictionary containing parameter values to substitute in the `path` string.
    :type params: Dict[str, Any]
    :return: A tuple containing two elements: A list of file paths matching the specified `path` pattern, and a set of parameter keys used in the `path` pattern.
    :rtype: Tuple[List[str], Set[str]]
    """
    deps = []
    param_keys = set()
    if isinstance(path, list):
        for p in path:
            rdeps, rparam_keys = get_deps(p, params)
            deps += rdeps
            param_keys |= rparam_keys
    else:
        path, matches = _parse_path(path, params)
        param_keys |= matches
        deps = glob.glob(path)

    deps = list(sorted(set(deps)))

    assert (
        len(deps) > 0
    ), f'Dependencies not found for path "{path}".\nIs DVC Pipeline up to date?'

    return deps, param_keys


def import_from_string(import_from):
    def import_from_string(import_from: str) -> Callable:
    """
    Import and return a callable function by name.

    :param import_from: A string representing the fully qualified name of the function.
    :type import_from: str

    :return: A callable function.
    :rtype: Callable
    """
    module_name, function_name = import_from.rsplit(".", 1)
    fn = getattr(importlib.import_module(module_name), function_name)
    return fn


def key_is_skipped(key, include, exclude):
    """
    Check if a key should be skipped based on include and exclude lists.

    :param key: The key to check.
    :type key: str
    :param include: The list of keys to include. If empty, include all keys.
    :type include: List[str]
    :param exclude: The list of keys to exclude. If empty, exclude no keys.
    :type exclude: List[str]
    :return: True if the key should be skipped, False otherwise.
    :rtype: bool
    """
    cond = (key in exclude) or (len(include) > 0 and key not in include)
    __LOGGER__.debug(f'key "{key}" is {"" if cond else "not "}skipped')
    return cond
