# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2022-11-29 10:47:10 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import glob
import importlib
import logging
import os

import dvc.api
import yaml
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

import dvc_stage
from dvc_stage.loading import load_data
from dvc_stage.transforming import apply_transformation
from dvc_stage.validating import validate_data
from dvc_stage.writing import write_data


# FUNCTION DEFINITIONS ########################################################
def flatten_dict(d, parent_key="", sep="."):
    items = []
    for k, v in d.items():
        new_key = sep.join((parent_key, k)) if parent_key else k
        if isinstance(v, dict) and len(v):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_outs(data, path, **kwds):
    outs = []

    if isinstance(data, list):
        logging.debug("data is list")
        for i, d in enumerate(data):
            outs.append(path.format(item=i))
        return outs
    if isinstance(data, dict):
        logging.debug("arg is dict")
        for k, v in data.items():
            outs.append(path.format(key=k))
        return outs
    else:
        logging.debug(f"path: {path}")
        return [path]


def get_deps(path):
    deps = []
    if isinstance(path, list):
        for p in paths:
            deps.append(get_deps(p))
    elif "*" in path:
        return list(set(map(os.path.dirname, glob.glob(path))))
    else:
        return [path]


def trace_transformations(params):
    arg = None

    write = params.get("write", None)
    if write is not None:
        transformations = params.get("transformations", None)
        if transformations is not None:
            for name, kwds in transformations.items():
                logging.debug(f"tracing transformation function {name}")
                arg = apply_transformation(name, arg, **kwds)

        outs = get_outs(data=arg, **params["write"])
        return outs
    else:
        return None


def get_dvc_config(stage):
    logging.debug(f"tracing dvc stage: {stage}")
    params = dvc.api.params_show()[stage]
    logging.debug(params)
    if "extra_modules" in params.keys():
        load_extra_modules(params["extra_modules"])

    config = params.get("extra_stage_fields", {})
    config.update(
        {
            "cmd": f"dvc-stage run {stage}",
            "deps": get_deps(params["load"]["path"]),
            "params": list(flatten_dict(params, parent_key=stage).keys()),
            "meta": {"dvc-stage-version": dvc_stage.__version__},
        }
    )
    outs = trace_transformations(params)
    if outs is not None:
        config["outs"] = outs
    config = {"stages": {stage: config}}
    return config


def print_stage_config(stage):
    config = get_dvc_config(stage)
    print(yaml.dump(config))


def load_dvc_yaml():
    logging.debug("loading dvc.yaml")
    with open("dvc.yaml", "r") as f:
        dvc_yaml = yaml.safe_load(f)
    logging.debug(dvc_yaml)
    return dvc_yaml


def check_dvc_yaml(stage):
    dvc_yaml = load_dvc_yaml()["stages"][stage]
    logging.debug(f"dvc.yaml:\n{yaml.dump(dvc_yaml)}")
    config = get_dvc_config(stage)["stages"][stage]
    logging.debug(f"expected:\n{yaml.dump(config)}")

    return dvc_yaml == config


def validate_dvc_yaml(stage):
    logging.debug("validating dvc.yaml")
    assert check_dvc_yaml(stage), f"dvc.yaml for {stage} is invalid."


def update_dvc_stage(stage):
    if check_dvc_yaml(stage):
        logging.info(f"stage difinition of {stage} is up to date")
    else:
        logging.info("updating dvc.yaml")
        dvc_yaml = load_dvc_yaml()
        config = get_dvc_config(stage)["stages"][stage]

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
    dvc_yaml = load_dvc_yaml()
    for stage, definition in dvc_yaml["stages"].items():
        if definition.get("cmd", "").startswith("dvc-stage"):
            update_dvc_stage(stage)


def load_extra_modules(extra_modules):
    for module_name in extra_modules:
        module = importlib.import_module(module_name)
        dvc_stage.loading.DATA_LOAD_FUNCTIONS.update(
            getattr(module, "DATA_LOAD_FUNCTIONS", {})
        )
        dvc_stage.transforming.TRANSFORMATION_FUNCTIONS.update(
            getattr(module, "TRANSFORMATION_FUNCTIONS", {})
        )
        dvc_stage.validating.VALIDATION_FUNCTIONS.update(
            getattr(module, "VALIDATION_FUNCTIONS", {})
        )
        dvc_stage.writing.DATA_WRITE_FUNCTIONS.update(
            getattr(module, "DATA_WRITE_FUNCTIONS", {})
        )


def run_stage(stage, validate=True):
    if validate:
        validate_dvc_yaml(stage)
    params = dvc.api.params_show(stages=stage)[stage]
    logging.debug(params)

    if "extra_modules" in params.keys():
        load_extra_modules(params["extra_modules"])

    arg = load_data(
        **params["load"],
    )

    transformations = params.get("transformations", None)
    if transformations is not None:
        logging.debug("applying transformations")
        logging.debug(transformations)
        it = tqdm(transformations.items())
        with logging_redirect_tqdm():
            for name, kwds in it:
                it.set_description(name)
                arg = apply_transformation(
                    name,
                    arg,
                    **kwds,
                )

    validations = params.get("validations", None)
    if validations is not None:
        logging.debug("applying validations")
        logging.debug(validations)
        it = tqdm(validations.items())
        with logging_redirect_tqdm():
            for name, kwds in it:
                it.set_description(name)
                arg = validate_data(
                    name,
                    arg,
                    **kwds,
                )

    write = params.get("write", None)
    if write is not None:
        arg = write_data(
            data=arg,
            **params["write"],
        )
