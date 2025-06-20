# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : utils.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 13:18:39 (Marcel Arpogaus)
# changed : 2025-06-20 11:16:03 (Marcel Arpogaus)

# %% Description ###############################################################
"""utils module."""

# %% imports ###################################################################
import glob
import importlib
import logging
import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import pandas as pd

# %% globals ###################################################################
__LOGGER__ = logging.getLogger(__name__)


# %% functions #################################################################
def parse_path(path: str, **params: Dict[str, Any]) -> Tuple[str, Set[str]]:
    """Parse a path and replace ${PLACEHOLDERS} with values from dict.

    Parameters
    ----------
    path : str
        The path string to parse.
    params : Dict[str, Any]
        A dictionary of parameter values to replace placeholders.

    Returns
    -------
    Tuple[str, Set[str]]
        A tuple containing the parsed path string and a set of the matched parameters.

    """
    pattern = re.compile(r"\${([a-zA-Z_][a-zA-Z0-9_]*)}")
    matches = set(re.findall(pattern, path))
    for g in matches:
        if g == "item" and not params.get("item", None):
            continue
        path = path.replace("${" + g + "}", params[g])
    return path, matches


def flatten_dict(
    d: Dict[str, Any], parent_key: str = "", sep: str = "."
) -> Dict[str, Any]:
    """Recursively flatten a nested dictionary into a single-level dictionary.

    Parameters
    ----------
    d : dict
        The dictionary to flatten.
    parent_key : str, optional
        The parent key for the current level of the dictionary.
    sep : str, optional
        The separator to use between keys.

    Returns
    -------
    dict
        The flattened dictionary.

    """
    items = []
    for k, v in d.items():
        new_key = sep.join((parent_key, k)) if parent_key else k
        if isinstance(v, dict) and len(v):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_deps(
    path: Union[str, List[str]], params: Dict[str, Any], item: Optional[str] = None
) -> Tuple[List[str], Set[str]]:
    """Get dependencies given a path pattern and parameter values.

    Parameters
    ----------
    path : str or List[str]
        A string or list of strings representing file paths.
    params : Dict[str, Any]
        A dictionary containing parameter values to substitute in the `path` string.
    item : str, optional
        Item identifier for foreach stages (default None).

    Returns
    -------
    Tuple[List[str], Set[str]]
        A tuple containing two elements:
          1. a list of file paths matching the specified `path` pattern.
          2. a set of parameter keys used in the `path` pattern.

    """
    deps = []
    param_keys = set()
    if isinstance(path, list):
        for p in path:
            rdeps, rparam_keys = get_deps(p, params, item)
            deps += rdeps
            param_keys |= rparam_keys
    else:
        path, matches = parse_path(path, item=item, **params)
        param_keys |= matches
        if "item" in matches and item is None:
            deps = [path]
            param_keys.remove("item")
        else:
            deps = glob.glob(path)

    deps = list(sorted(set(deps)))

    assert len(deps) > 0, (
        f'Dependencies not found for path "{path}".\nIs DVC Pipeline up to date?'
    )

    return deps, param_keys


def get_outs(
    data: Union[List, Dict, pd.DataFrame], path: str, **kwds: Any
) -> List[str]:
    """Get list of output paths based on input data.

    Parameters
    ----------
    data : Union[List, Dict, pd.DataFrame]
        Input data.
    path : str
        Output path template string.
    kwds : Any
        Additional keyword arguments.

    Returns
    -------
    List[str]
        List of output paths.

    """
    outs = []

    if isinstance(data, dict):
        __LOGGER__.debug("arg is dict")
        for k, v in data.items():
            outs.append(parse_path(path, key=k)[0])
    else:
        __LOGGER__.debug(f"path: {path}")
        outs.append(path)

    return list(sorted(outs))


def import_from_string(import_from: str) -> Callable:
    """Import and return a callable function by name.

    Parameters
    ----------
    import_from : str
        A string representing the fully qualified name of the function.

    Returns
    -------
    Callable
        A callable function.

    """
    module_name, function_name = import_from.rsplit(".", 1)
    fn = getattr(importlib.import_module(module_name), function_name)
    return fn


def key_is_skipped(key: str, include: List[str], exclude: List[str]) -> bool:
    """Check if a key should be skipped based on include and exclude lists.

    Parameters
    ----------
    key : str
        The key to check.
    include : List[str]
        The list of keys to include. If empty, include all keys.
    exclude : List[str]
        The list of keys to exclude. If empty, exclude no keys.

    Returns
    -------
    bool
        True if the key should be skipped, False otherwise.

    """
    cond = re.fullmatch("|".join(exclude), key) or (
        len(include) > 0 and not re.fullmatch("|".join(include), key)
    )
    __LOGGER__.debug(f'key "{key}" is {"" if cond else "not "}skipped')
    return cond
