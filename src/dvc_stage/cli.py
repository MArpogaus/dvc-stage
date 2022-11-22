# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# AUTHOR INFORMATION ##########################################################
# file    : dvc_stage.py
# author  : Marcel Arpogaus <marcel dot arpogaus at gmail dot com>
#
# created : 2022-11-15 08:02:51 (Marcel Arpogaus)
# changed : 2022-11-22 15:10:25 (Marcel Arpogaus)
# DESCRIPTION #################################################################
# ...
# LICENSE #####################################################################
# ...
###############################################################################
# REQUIRED MODULES ############################################################
import argparse
import logging
import os
import sys

import dvc.api
import yaml
from flatten_dict import flatten
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from dvc_stage.loading import load_data
from dvc_stage.transforming import apply_transformation
from dvc_stage.writing import write_data


# FUNCTION DEFINITIONS ########################################################
def get_dvc_config(dvc_stage):
    params = dvc.api.params_show()[dvc_stage]
    config = {
        "stages": {
            dvc_stage: {
                "cmd": f"dvc_stage {dvc_stage}",
                "deps": [params["load"]["path"]],
                "params": [
                    ".".join((dvc_stage, k))
                    for k in flatten(params, reducer="dot").keys()
                ],
                "outs": [os.path.dirname(params["write"]["path"])],
            }
        }
    }
    return config


def print_dvc_config(dvc_stage):
    config = get_dvc_config(dvc_stage)
    print(yaml.dump(config))


def run_stage(dvc_stage):
    params = dvc.api.params_show(stages=dvc_stage)[dvc_stage]
    logging.debug(params)

    arg = load_data(**params["load"])

    it = tqdm(params["transformations"].items())
    with logging_redirect_tqdm():
        for name, kwds in it:
            it.set_description(name)
            arg = apply_transformation(name, arg, **kwds)
    arg = write_data(data=arg, **params["write"])


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("dvc_stage", help="Name of DVC stage the script is used in")
    parser.add_argument(
        "--log-file",
        type=argparse.FileType("a"),
        help="Path to logfile",
    )
    parser.add_argument(
        "--log-level", type=str, default="info", help="Provide logging level."
    )
    parser.add_argument(
        "--get-config",
        action="store_true",
        default=False,
        help="Generate dvc.yaml entry",
    )
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

    dvc_stage = args.dvc_stage
    if args.get_config:
        print_dvc_config(dvc_stage)
    else:
        run_stage(dvc_stage)


if __name__ == "__main__":
    cli()
