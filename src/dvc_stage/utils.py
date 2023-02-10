# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-10 13:13:15 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import glob
import logging
import re
from typing import Dict

import dvc.api
import yaml

import dvc_stage
from dvc_stage.loading import load_data
from dvc_stage.transforming import apply_transformations
from dvc_stage.validating import apply_validations
from dvc_stage.writing import get_outs, write_data

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _flatten_dict(d, parent_key="", sep="."):
    items = []
    for k, v in d.items():
        new_key = sep.join((parent_key, k)) if parent_key else k
        if isinstance(v, dict) and len(v):
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _load_dvc_yaml():
    __LOGGER__.debug("loading dvc.yaml")
    with open("dvc.yaml", "r") as f:
        dvc_yaml = yaml.safe_load(f)
    __LOGGER__.debug(dvc_yaml)
    return dvc_yaml


def _check_dvc_yaml(stage):
    dvc_yaml = _load_dvc_yaml()["stages"][stage]
    __LOGGER__.debug(f"dvc.yaml:\n{yaml.dump(dvc_yaml)}")
    config = _get_dvc_config(stage)["stages"][stage]
    if stage in dvc_yaml["cmd"]:
        config["cmd"] = dvc_yaml["cmd"]
    __LOGGER__.debug(f"expected:\n{yaml.dump(config)}")

    return dvc_yaml == config


def _validate_dvc_yaml(stage):
    __LOGGER__.debug("validating dvc.yaml")
    assert _check_dvc_yaml(stage), f"dvc.yaml for {stage} is invalid."


def _parse_path(path, params) -> Dict:
    print(path, params)
    pattern = re.compile(r"\${([a-z]+)}")  # noqa: W605
    matches = set(re.findall(pattern, path))
    print(matches)
    for g in matches:
        path = path.replace("${" + g + "}", params[g])
    print(path)
    return path, matches


def _get_deps(path, params):
    deps = []
    param_keys = set()
    if isinstance(path, list):
        for p in path:
            rdeps, rparam_keys = _get_deps(p, params)
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


def _get_params(stage, all=False):
    params = dvc.api.params_show(stages=None if all else stage)
    stage_params = params[stage]
    global_params = dict(
        filter(lambda kv: isinstance(kv[1], (int, float, str)), params.items())
    )
    __LOGGER__.debug(stage_params, global_params)

    return stage_params, global_params


def _get_dvc_config(stage):
    __LOGGER__.debug(f"tracing dvc stage: {stage}")

    stage_params, global_params = _get_params(stage, all=all)

    dvc_params = list(_flatten_dict(stage_params, parent_key=stage).keys())
    deps, param_keys = _get_deps(stage_params["load"].pop("path"), global_params)
    dvc_params += list(param_keys)

    config = stage_params.get("extra_stage_fields", {})
    config.update(
        {
            "cmd": f"dvc-stage run {stage}",
            "deps": deps + stage_params.get("extra_deps", []),
            "params": list(sorted(dvc_params)),
            "meta": {"dvc-stage-version": dvc_stage.__version__},
        }
    )

    transformations = stage_params.get("transformations", None)
    write = stage_params.get("write", None)
    load = stage_params["load"]

    # if the format is None data loading is skipped and None is returned tracing
    load["format"] = None

    data = load_data(path=deps, **load)

    if transformations is not None:
        assert write is not None, "No writer configured."
        data = apply_transformations(data, transformations)
        outs = get_outs(data, **write)
        config["outs"] = outs + stage_params.get("extra_outs", [])

    config = {"stages": {stage: config}}

    return config


# PUBLIC FUNCTIONS ############################################################
def print_stage_config(stage):
    config = _get_dvc_config(stage)
    print(yaml.dump(config))


def update_dvc_stage(stage):
    if _check_dvc_yaml(stage):
        __LOGGER__.info(f"stage definition of {stage} is up to date")
    else:
        __LOGGER__.info("updating dvc.yaml")
        dvc_yaml = _load_dvc_yaml()
        config = _get_dvc_config(stage)["stages"][stage]
        if stage in dvc_yaml["stages"][stage]["cmd"]:
            config["cmd"] = dvc_yaml["stages"][stage]["cmd"]

        __LOGGER__.info(f"before:\n{yaml.dump(dvc_yaml['stages'][stage])}")
        __LOGGER__.info(f"after update:\n{yaml.dump(config)}")
        __LOGGER__.warn("This will alter your dvc.yaml")
        answer = input("type [y]es to continue: ")

        if answer.lower() in ["y", "yes"]:
            dvc_yaml["stages"][stage] = config
            with open("dvc.yaml", "w") as f:
                yaml.dump(dvc_yaml, f, sort_keys=False)
            __LOGGER__.info("dvc.yaml successfully updated")
        else:
            __LOGGER__.error("Operation canceled by user")
            exit(1)


def update_dvc_yaml():
    dvc_yaml = _load_dvc_yaml()
    for stage, definition in dvc_yaml["stages"].items():
        if definition.get("cmd", "").startswith("dvc-stage"):
            update_dvc_stage(stage)


def run_stage(stage, validate=True):
    if validate:
        _validate_dvc_yaml(stage)

    stage_params, global_params = _get_params(stage)
    __LOGGER__.debug(stage_params)

    deps, _ = _get_deps(stage_params["load"].pop("path"), global_params)
    data = load_data(
        path=deps,
        **stage_params["load"],
    )

    transformations = stage_params.get("transformations", None)
    validations = stage_params.get("validations", None)
    write = stage_params.get("write", None)

    if transformations is not None:
        assert write is not None, "No writer configured."
        data = apply_transformations(data, transformations)

    if validations is not None:
        apply_validations(data, validations)

    if write is not None:
        write_data(
            data=data,
            **stage_params["write"],
        )
