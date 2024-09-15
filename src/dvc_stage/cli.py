# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : cli.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 13:43:10 (Marcel Arpogaus)
# changed : 2024-09-15 13:44:11 (Marcel Arpogaus)

# %% Description ###############################################################
"""cli module."""

# %% imports ###################################################################
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

# %% globals ###################################################################
__LOGGER__ = logging.getLogger(__name__)


# %% private functions #########################################################
def _print_stage_definition(stage: str) -> None:
    """Print the stage definition for the specified DVC stage in YAML format.

    Parameters
    ----------
    stage : str
        The name of the DVC stage to retrieve the definition for.

    """
    config = get_stage_definition(stage)
    print(yaml.dump(config))


def _update_dvc_stage(stage: str, yes: bool) -> None:
    """Update the definition in the `dvc.yaml` file for the specified DVC stage.

    Parameters
    ----------
    stage : str
        The name of the DVC stage to update.
    yes : bool
        Whether to continue without asking for confirmation.

    """
    if stage_definition_is_valid(stage):
        __LOGGER__.info(f"stage definition of {stage} is up to date")
    else:
        __LOGGER__.info(
            f"stage definition of {stage} is invalid, dvc.yaml needs to be updated"
        )
        dvc_yaml = load_dvc_yaml()
        config = get_stage_definition(stage)["stages"][stage]
        if stage in dvc_yaml["stages"]:
            config["cmd"] = dvc_yaml["stages"][stage]["cmd"]

        s1 = yaml.dump(dvc_yaml["stages"][stage]).splitlines()
        s2 = yaml.dump(config).splitlines()
        diff = difflib.ndiff(s1, s2)
        diff_str = "\n".join(diff)
        __LOGGER__.info(f"changes:\n{diff_str}")

        if not yes:
            __LOGGER__.warning("This will alter your dvc.yaml")
            answer = input("type [y]es to continue: ")
        else:
            answer = "y"

        if answer.lower() in ["y", "yes"]:
            dvc_yaml["stages"][stage] = config
            with open("dvc.yaml", "w") as f:
                yaml.dump(dvc_yaml, f, sort_keys=False)
            __LOGGER__.info("dvc.yaml successfully updated")
        else:
            __LOGGER__.error("Operation canceled by user")
            sys.exit(1)


def _update_dvc_yaml(yes: bool) -> None:
    """Update all DVC stages defined in the `dvc.yaml` file.

    Parameters
    ----------
    yes : bool
        Whether to continue without asking for confirmation.

    """
    dvc_yaml = load_dvc_yaml()
    for stage, definition in dvc_yaml["stages"].items():
        if definition.get("cmd", "").startswith("dvc-stage"):
            _update_dvc_stage(stage, yes)


def _run_stage(stage: str, validate: bool = True) -> None:
    """Load, apply transformations, validate and write output.

    Parameters
    ----------
    stage : str
        The name of the DVC stage to run.
    validate : bool, optional
        Whether to validate the stage definition before running (default True).

    """
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

    transformations = stage_params.get("transformations")
    validations = stage_params.get("validations")
    write = stage_params.get("write")

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


# %% public functions ##########################################################
def cli() -> None:
    """Define the command-line interface for this script."""
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
    update_cfg_parser.add_argument(
        "-y",
        "--yes",
        help="Continue without asking for confirmation.",
        action="store_true",
        default=False,
    )
    update_cfg_parser.set_defaults(func=_update_dvc_stage)

    update_all_parser = subparsers.add_parser(
        "update-all", help="update all dvc stages in dvc.yaml"
    )
    update_all_parser.add_argument(
        "-y",
        "--yes",
        help="Continue without asking for confirmation.",
        action="store_true",
        default=False,
    )
    update_all_parser.set_defaults(func=_update_dvc_yaml)

    args = parser.parse_args()

    # Configure logging
    handlers = [
        logging.StreamHandler(sys.stdout),
    ]

    if args.log_file is not None:
        handlers.append(logging.StreamHandler(args.log_file))

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )

    kwds = {
        k: v
        for k, v in vars(args).items()
        if k not in ("log_level", "log_file", "func")
    }

    args.func(**kwds)


if __name__ == "__main__":
    cli()
