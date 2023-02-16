# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2023-02-16 12:44:16 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import argparse
import difflib
import logging
import sys

import yaml

from dvc_stage.config import (
    get_stage_definition,
    get_stage_params,
    load_dvc_yaml,
    stage_definition_is_valid,
    validate_stage_definition,
)
from dvc_stage.loading import load_data
from dvc_stage.transforming import apply_transformations
from dvc_stage.utils import get_deps
from dvc_stage.validating import apply_validations
from dvc_stage.writing import write_data

# MODULE GLOBAL VARIABLES #####################################################
__LOGGER__ = logging.getLogger(__name__)


# PRIVATE FUNCTIONS ###########################################################
def _print_stage_definition(stage):
    config = get_stage_definition(stage)
    print(yaml.dump(config))


def _update_dvc_stage(stage):
    if stage_definition_is_valid(stage):
        __LOGGER__.info(f"stage definition of {stage} is up to date")
    else:
        __LOGGER__.info(
            f"stage definition of {stage} is invalid, dvc.yaml need to be updated"
        )
        dvc_yaml = load_dvc_yaml()
        config = get_stage_definition(stage)["stages"][stage]
        if stage in dvc_yaml["stages"][stage]["cmd"]:
            config["cmd"] = dvc_yaml["stages"][stage]["cmd"]

        s1 = yaml.dump(dvc_yaml["stages"][stage]).splitlines()
        s2 = yaml.dump(config).splitlines()
        diff = difflib.ndiff(s1, s2)
        diff = "\n".join(diff)
        __LOGGER__.info(f"changes:\n{diff}")

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


def _update_dvc_yaml():
    dvc_yaml = load_dvc_yaml()
    for stage, definition in dvc_yaml["stages"].items():
        if definition.get("cmd", "").startswith("dvc-stage"):
            _update_dvc_stage(stage)


def _run_stage(stage, validate=True):
    if validate:
        validate_stage_definition(stage)

    stage_params, global_params = get_stage_params(stage)
    __LOGGER__.debug(stage_params)

    deps, _ = get_deps(stage_params["load"].pop("path"), global_params)

    __LOGGER__.info("loading data")
    data = load_data(
        paths=deps,
        **stage_params["load"],
    )
    __LOGGER__.info("all data loaded")

    transformations = stage_params.get("transformations", None)
    validations = stage_params.get("validations", None)
    write = stage_params.get("write", None)

    if transformations is not None:
        assert write is not None, "No writer configured."
        __LOGGER__.info("applying transformations")
        data = apply_transformations(data, transformations)
        __LOGGER__.info("all transformations applied")

    if validations is not None:
        __LOGGER__.info("applying validations")
        apply_validations(data, validations)
        __LOGGER__.info("all validations passed")

    if write is not None:
        __LOGGER__.info("writing data")
        write_data(
            data=data,
            **stage_params["write"],
        )
        __LOGGER__.info("all data written")


# PUBLIC FUNCTIONS ############################################################
def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-file",
        type=argparse.FileType("a"),
        help="Path to logfile",
    )
    parser.add_argument(
        "--log-level", type=str, default="info", help="Provide logging level."
    )

    subparsers = parser.add_subparsers(title="subcommands", help="valid subcommands")
    run_parser = subparsers.add_parser("run", help="run given stage")
    run_parser.add_argument("stage", help="Name of DVC stage the script is used in")
    validate_dvc_yaml_parser = run_parser.add_mutually_exclusive_group(
        required=False,
    )
    validate_dvc_yaml_parser.add_argument(
        "--skip-validation",
        dest="validate",
        action="store_false",
        help="do not validate stage definition in dvc.yaml",
    )
    run_parser.set_defaults(func=_run_stage)

    get_cfg_parser = subparsers.add_parser("get-config", help="get dvc config")
    get_cfg_parser.add_argument("stage", help="Name of DVC stage the script is used in")
    get_cfg_parser.set_defaults(func=_print_stage_definition)

    update_cfg_parser = subparsers.add_parser("update-stage", help="update dvc config")
    update_cfg_parser.add_argument(
        "stage", help="Name of DVC stage the script is used in"
    )
    update_cfg_parser.set_defaults(func=_update_dvc_stage)

    update_all_parser = subparsers.add_parser("update-all", help="update dvc config")
    update_all_parser.set_defaults(func=_update_dvc_yaml)

    args = parser.parse_args()

    # Configure logging
    handlers = [
        logging.StreamHandler(sys.stdout),
    ]

    if args.log_file is not None:
        handlers += [logging.StreamHandler(args.log_file)]

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )

    kwds = dict(
        filter(
            lambda kw: kw[0] not in ("log_level", "log_file", "func"),
            vars(args).items(),
        )
    )

    args.func(**kwds)


if __name__ == "__main__":
    cli()
