# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-09 15:51:34 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import logging

import dvc.api
import yaml

import dvc_stage
from dvc_stage.loading import get_deps, load_data
from dvc_stage.transforming import apply_transformations
from dvc_stage.validating import apply_validations
from dvc_stage.writing import get_outs, write_data


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
    logging.debug("loading dvc.yaml")
    with open("dvc.yaml", "r") as f:
        dvc_yaml = yaml.safe_load(f)
    logging.debug(dvc_yaml)
    return dvc_yaml


def _check_dvc_yaml(stage):
    dvc_yaml = _load_dvc_yaml()["stages"][stage]
    logging.debug(f"dvc.yaml:\n{yaml.dump(dvc_yaml)}")
    config = _get_dvc_config(stage)["stages"][stage]
    if stage in dvc_yaml["cmd"]:
        config["cmd"] = dvc_yaml["cmd"]
    logging.debug(f"expected:\n{yaml.dump(config)}")

    return dvc_yaml == config


def _validate_dvc_yaml(stage):
    logging.debug("validating dvc.yaml")
    assert _check_dvc_yaml(stage), f"dvc.yaml for {stage} is invalid."


def _get_dvc_config(stage):
    logging.debug(f"tracing dvc stage: {stage}")
    params = dvc.api.params_show()[stage]
    logging.debug(params)

    deps = get_deps(params["load"]["path"])

    config = params.get("extra_stage_fields", {})
    config.update(
        {
            "cmd": f"dvc-stage run {stage}",
            "deps": deps + params.get("extra_deps", []),
            "params": list(_flatten_dict(params, parent_key=stage).keys()),
            "meta": {"dvc-stage-version": dvc_stage.__version__},
        }
    )

    transformations = params.get("transformations", None)
    write = params.get("write", None)
    load = params["load"]

    # if the format is None data loading is skipped and None is returned tracing
    load["format"] = None

    data = load_data(**load)

    if transformations is not None:
        assert write is not None, "No writer configured."
        data = apply_transformations(data, transformations)
        outs = get_outs(data, **write)
        config["outs"] = outs + params.get("extra_outs", [])

    config = {"stages": {stage: config}}

    return config


# PUBLIC FUNCTIONS ############################################################
def print_stage_config(stage):
    config = _get_dvc_config(stage)
    print(yaml.dump(config))


def update_dvc_stage(stage):
    if _check_dvc_yaml(stage):
        logging.info(f"stage definition of {stage} is up to date")
    else:
        logging.info("updating dvc.yaml")
        dvc_yaml = _load_dvc_yaml()
        config = _get_dvc_config(stage)["stages"][stage]
        if stage in dvc_yaml["stages"][stage]["cmd"]:
            config["cmd"] = dvc_yaml["stages"][stage]["cmd"]

        logging.info(f"before:\n{yaml.dump(dvc_yaml['stages'][stage])}")
        logging.info(f"after update:\n{yaml.dump(config)}")
        logging.warn("This will alter your dvc.yaml")
        answer = input("type [y]es to continue: ")

        if answer.lower() in ["y", "yes"]:
            dvc_yaml["stages"][stage] = config
            with open("dvc.yaml", "w") as f:
                yaml.dump(dvc_yaml, f, sort_keys=False)
            logging.info("dvc.yaml successfully updated")
        else:
            logging.error("Operation canceled by user")
            exit(1)


def update_dvc_yaml():
    dvc_yaml = _load_dvc_yaml()
    for stage, definition in dvc_yaml["stages"].items():
        if definition.get("cmd", "").startswith("dvc-stage"):
            update_dvc_stage(stage)


def run_stage(stage, validate=True):
    if validate:
        _validate_dvc_yaml(stage)
    params = dvc.api.params_show(stages=stage)[stage]
    logging.debug(params)

    data = load_data(
        **params["load"],
    )

    transformations = params.get("transformations", None)
    validations = params.get("validations", None)
    write = params.get("write", None)

    if transformations is not None:
        assert write is not None, "No writer configured."
        data = apply_transformations(data, transformations)

    if validations is not None:
        apply_validations(data, validations)

    if write is not None:
        write_data(
            data=data,
            **params["write"],
        )
