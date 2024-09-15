# -*- time-stamp-pattern: "changed[\s]+:[\s]+%%$"; -*-
# %% Author ####################################################################
# file    : __init__.py
# author  : Marcel Arpogaus <znepry.necbtnhf@tznvy.pbz>
#
# created : 2024-09-15 14:06:21 (Marcel Arpogaus)
# changed : 2024-09-15 14:14:41 (Marcel Arpogaus)

# %% Description ###############################################################
""".. include:: ../../README.md"""  # noqa: D415,D400

# %% imports ###################################################################
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dvc-stage")
except PackageNotFoundError:
    __version__ = "unknown version"
