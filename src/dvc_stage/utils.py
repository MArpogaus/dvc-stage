# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2022-11-23 14:56:48 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import glob
import importlib
import logging

import dvc.api
import yaml
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

import dvc_stage
from dvc_stage.loading import load_data
from dvc_stage.transforming import apply_transformation
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
    return glob.glob(path)


def trace_transformations(params, custom_transformation_functions):
    arg = None

    for name, kwds in params["transformations"].items():
        logging.debug(f"tracing transformation function {name}")
        arg = apply_transformation(name, arg, custom_transformation_functions, **kwds)

    outs = get_outs(data=arg, **params["write"])
    return outs


def get_dvc_config(stage):
    logging.debug(f"tracing dvc stage: {stage}")
    params = dvc.api.params_show()[stage]
    logging.debug(params)
    if "custom_functions" in params.keys():
        custom_functions = importlib.import_module(params["custom_functions"])
    else:
        custom_functions = {}

    config = {
        "stages": {
            stage: {
                "cmd": f"dvc-stage {stage}",
                "deps": get_deps(params["load"]["path"]),
                "params": list(flatten_dict(params, parent_key=stage).keys()),
                "outs": trace_transformations(
                    params,
                    custom_transformation_functions=getattr(
                        custom_functions, "TRANSFORMATION_FUNCTIONS", {}
                    ),
                ),
                "meta": {"dvc-stage-version": dvc_stage.__version__},
            }
        }
    }
    return config


def print_stage_config(stage):
    config = get_dvc_config(stage)
    print(yaml.dump(config))


def check_dvc_yaml(stage):
    with open("dvc.yaml", "r") as f:
        dvc_yaml = yaml.safe_load(f)["stages"][stage]
    logging.debug(f"dvc.yaml:\n{yaml.dump(dvc_yaml)}")
    config = get_dvc_config(stage)["stages"][stage]
    logging.debug(f"expected:\n{yaml.dump(config)}")

    return dvc_yaml == config


def validate_dvc_yaml(stage):
    logging.debug("validating dvc.yaml")
    assert check_dvc_yaml(stage), f"dvc.yaml for {stage} is invalid."


def update_dvc_yaml(stage):
    if check_dvc_yaml(stage):
        logging.info("dvc.yaml is up to date")
    else:
        logging.info("updating dvc.yaml")

        with open("dvc.yaml", "r") as f:
            dvc_yaml = yaml.safe_load(f)

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


def run_stage(stage, validate=True):
    if validate:
        validate_dvc_yaml(stage)
    params = dvc.api.params_show(stages=stage)[stage]
    logging.debug(params)

    if "custom_functions" in params.keys():
        custom_functions = importlib.import_module(params["custom_functions"])
    else:
        custom_functions = {}

    arg = load_data(
        custom_data_load_functions=getattr(custom_functions, "DATA_LOAD_FUNCTIONS", {}),
        **params["load"],
    )

    it = tqdm(params["transformations"].items())
    with logging_redirect_tqdm():
        for name, kwds in it:
            it.set_description(name)
            arg = apply_transformation(
                name,
                arg,
                custom_transformation_functions=getattr(
                    custom_functions, "TRANSFORMATION_FUNCTIONS", {}
                ),
                **kwds,
            )
    arg = write_data(
        data=arg,
        custom_data_write_functions=getattr(
            custom_functions, "DATA_WRITE_FUNCTIONS", {}
        ),
        **params["write"],
    )
