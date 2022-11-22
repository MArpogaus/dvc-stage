# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2022-11-22 16:44:57 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import argparse
import logging
import sys

from dvc_stage.utils import print_dvc_config, run_stage, update_dvc_config


# FUNCTION DEFINITIONS ########################################################
def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-file",
        type=argparse.FileType("a"),
        help="Path to logfile",
    )
    parser.add_argument("dvc_stage", help="Name of DVC stage the script is used in")
    parser.add_argument(
        "--log-level", type=str, default="info", help="Provide logging level."
    )
    parser.set_defaults(func=run_stage)

    subparsers = parser.add_subparsers(title="subcommands", help="valid subcommands")
    run_parser = subparsers.add_parser("run", help="run given stage")
    run_parser.set_defaults(func=run_stage)

    get_cfg_parser = subparsers.add_parser("get-config", help="get dvc config")
    get_cfg_parser.set_defaults(func=print_dvc_config)

    update_cfg_parser = subparsers.add_parser("update-config", help="update dvc config")
    update_cfg_parser.set_defaults(func=update_dvc_config)

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
    args.func(args.dvc_stage)


if __name__ == "__main__":
    cli()
