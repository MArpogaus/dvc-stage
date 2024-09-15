# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : config.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 13:48:10 (Marcel Arpogaus)
# changed : 2024-09-15 13:49:32 (Marcel Arpogaus)

# %% Description ###############################################################
"""config module."""

# %% imports ###################################################################
import logging
from typing import Any, Dict, Tuple

import dvc.api
import yaml

import dvc_stage
from dvc_stage.loading import load_data
from dvc_stage.transforming import apply_transformations
from dvc_stage.utils import flatten_dict, get_deps
from dvc_stage.writing import get_outs

# %% globals ###################################################################
__LOGGER__ = logging.getLogger(__name__)


# %% functions #################################################################
def load_dvc_yaml() -> Dict[str, Any]:
    """Load and return the dvc.yaml file as a dictionary.

    Returns
    -------
    dict
        The contents of dvc.yaml file.

    """
    __LOGGER__.debug("loading dvc.yaml")
    with open("dvc.yaml", "r") as f:
        dvc_yaml = yaml.safe_load(f)
    __LOGGER__.debug(dvc_yaml)
    return dvc_yaml


def get_stage_definition(stage: str) -> Dict[str, Any]:
    """Generate a dvc stage definition dictionary based on the given stage name.

    Parameters
    ----------
    stage : str
        The name of the dvc stage.

    Returns
    -------
    dict
        The dvc stage definition dictionary.

    """
    __LOGGER__.debug(f"tracing dvc stage: {stage}")

    stage_params, global_params = get_stage_params(stage, all=True)

    dvc_params = list(flatten_dict(stage_params, parent_key=stage).keys())
    deps, param_keys = get_deps(stage_params["load"].pop("path"), global_params)
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

    data = load_data(paths=deps, quiet=True, **load)

    if transformations is not None:
        assert write is not None, "No writer configured."
        data = apply_transformations(data, transformations, quiet=True)
        outs = get_outs(data, **write)
        config["outs"] = outs + stage_params.get("extra_outs", [])

    config = {"stages": {stage: config}}

    return config


def stage_definition_is_valid(stage: str) -> bool:
    """Check if the dvc.yaml file for the given stage is valid.

    Parameters
    ----------
    stage : str
        The name of the dvc stage.

    Returns
    -------
    bool
        True if dvc.yaml is valid.

    """
    dvc_yaml = load_dvc_yaml()["stages"][stage]
    __LOGGER__.debug(f"dvc.yaml:\n{yaml.dump(dvc_yaml)}")
    config = get_stage_definition(stage)["stages"][stage]
    if stage in dvc_yaml["cmd"]:
        config["cmd"] = dvc_yaml["cmd"]
    __LOGGER__.debug(f"expected:\n{yaml.dump(config)}")

    return dvc_yaml == config


def validate_stage_definition(stage: str) -> None:
    """Validate the dvc.yaml file for the given stage.

    Parameters
    ----------
    stage : str
        The name of the dvc stage.

    Note
    ----
    Raises AssertionError if invalid

    """
    __LOGGER__.debug("validating dvc.yaml")
    assert stage_definition_is_valid(stage), f"dvc.yaml for {stage} is invalid."


def get_stage_params(
    stage: str, all: bool = False
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Retrieve and return the stage parameters and global parameters as a tuple.

    Parameters
    ----------
    stage : str
        The name of the dvc stage.
    all : bool, optional
        If True, retrieve all stages' parameters. Defaults to False.

    Returns
    -------
    tuple
        A tuple (stage_params, global_params) containing the
        stage parameters and global parameters as dictionaries.

    """
    params = dvc.api.params_show(stages=None if all else stage)
    stage_params = params[stage]
    global_params = dict(
        filter(lambda kv: isinstance(kv[1], (int, float, str)), params.items())
    )
    __LOGGER__.debug(stage_params, global_params)

    return stage_params, global_params
