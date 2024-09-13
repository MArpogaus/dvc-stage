""".. include:: ../../README.md"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dvc-stage")
except PackageNotFoundError:
    __version__ = "unknown version"
