# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : utils.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 13:18:39 (Marcel Arpogaus)
# changed : 2025-06-20 14:56:10 (Marcel Arpogaus)

# %% Description ###############################################################
"""utils module."""

# %% imports ###################################################################
from __future__ import annotations

import glob
import importlib
import logging
import re

import pandas as pd

# %% globals ###################################################################
__LOGGER__ = logging.getLogger(__name__)


# %% functions #################################################################
def parse_path(path: str, **params: dict[str, any]) -> tuple[str, set[str]]:
    """Parse a path and replace ${PLACEHOLDERS} with values from dict.

    Parameters
    ----------
    path : str
        The path string to parse.
    **params : dict[str, any]
        A dictionary of parameter values to replace placeholders.

    Returns
    -------
    tuple[str, set[str]]
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
    d: dict[str, any], parent_key: str = "", sep: str = "."
) -> dict[str, any]:
    """Recursively flatten a nested dictionary into a single-level dictionary.

    Parameters
    ----------
    d : dict[str, any]
        The dictionary to flatten.
    parent_key : str, optional
        The parent key for the current level of the dictionary. Default is "".
    sep : str, optional
        The separator to use between keys. Default is ".".

    Returns
    -------
    dict[str, any]
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
    path: str | list[str], params: dict[str, any], item: str | None = None
) -> tuple[list[str], set[str]]:
    """Get dependencies given a path pattern and parameter values.

    Parameters
    ----------
    path : str | list[str]
        A string or list of strings representing file paths.
    params : dict[str, any]
        A dictionary containing parameter values to substitute in the path string.
    item : str | None, optional
        Item identifier for foreach stages. Default is None.

    Returns
    -------
    tuple[list[str], set[str]]
        A tuple containing two elements:
        1. A list of file paths matching the specified path pattern.
        2. A set of parameter keys used in the path pattern.

    Raises
    ------
    AssertionError
        If no dependencies are found for the given path.

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


def get_outs(data: list | dict | pd.DataFrame, path: str, **kwds: any) -> list[str]:
    """Get list of output paths based on input data.

    Parameters
    ----------
    data : list | dict | pd.DataFrame
        Input data.
    path : str
        Output path template string.
    **kwds : any
        Additional keyword arguments.

    Returns
    -------
    list[str]
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


def import_from_string(import_from: str) -> callable:
    """Import and return a callable function by name.

    Parameters
    ----------
    import_from : str
        A string representing the fully qualified name of the function.

    Returns
    -------
    callable
        A callable function.

    Raises
    ------
    AttributeError
        If the function cannot be imported.

    """
    module_name, function_name = import_from.rsplit(".", 1)
    fn = getattr(importlib.import_module(module_name), function_name)
    return fn


def key_is_skipped(key: str, include: list[str], exclude: list[str]) -> bool:
    """Check if a key should be skipped based on include and exclude lists.

    Parameters
    ----------
    key : str
        The key to check.
    include : list[str]
        The list of keys to include. If empty, include all keys.
    exclude : list[str]
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
