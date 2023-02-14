# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-14 15:48:06 (Marcel Arpogaus)
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
    pattern = re.compile(r"\${([a-z]+)}")  # noqa: W605
    matches = set(re.findall(pattern, path))
    for g in matches:
        path = path.replace("${" + g + "}", params[g])
    return path, matches


# PUBLIC FUNCTIONS ############################################################
def flatten_dict(d, parent_key="", sep="."):
    items = []
    for k, v in d.items():
        new_key = sep.join((parent_key, k)) if parent_key else k
        if isinstance(v, dict) and len(v):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_deps(path, params):
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


def import_custom_function(import_from):
    module_name, function_name = import_from.rsplit(".", 1)
    fn = getattr(importlib.import_module(module_name), function_name)
    return fn


def key_is_skipped(key, include, exclude):
    cond = (key in exclude) or (len(include) > 0 and key not in include)
    __LOGGER__.debug(f'key "{key}" is {"" if cond else "not "}skipped')
    return cond
