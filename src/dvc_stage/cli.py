# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2022-11-23 14:46:08 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import argparse
import logging
import sys

from dvc_stage.utils import print_stage_config, run_stage, update_dvc_yaml


# FUNCTION DEFINITIONS ########################################################
def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-file",
        type=argparse.FileType("a"),
        help="Path to logfile",
    )
    parser.add_argument("stage", help="Name of DVC stage the script is used in")
    parser.add_argument(
        "--log-level", type=str, default="info", help="Provide logging level."
    )
    parser.set_defaults(func=run_stage)

    subparsers = parser.add_subparsers(title="subcommands", help="valid subcommands")
    run_parser = subparsers.add_parser("run", help="run given stage")
    validate_dvc_yaml_parser = run_parser.add_mutually_exclusive_group(
        required=False,
    )
    validate_dvc_yaml_parser.add_argument(
        "--skip-validation",
        dest="validate",
        action="store_false",
        help="do not validate stage definition in dvc.yaml",
    )
    run_parser.set_defaults(func=run_stage)

    get_cfg_parser = subparsers.add_parser("get-config", help="get dvc config")
    get_cfg_parser.set_defaults(func=print_stage_config)

    update_cfg_parser = subparsers.add_parser("update-config", help="update dvc config")
    update_cfg_parser.set_defaults(func=update_dvc_yaml)

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
