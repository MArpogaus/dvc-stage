## v1.1.0 (2025-06-20)

### Feat

- allow transformation functions to process the raw data dict
- allow validation functions to process the raw data dict
- add support for simple foreach stages
- add dvc-stage and run command cli args as configuration option
- allow regexp for in and exclude fields

### Fix

- correct dvc.yaml format
- split transforms now work as expected
- use parse_path for variable expansion within paths
- detect dvc-stage definitions with foreach loop
- keep data paths as list to ensure keys are used
- use default logging format to include module name

### Refactor

- move get_outs into utils

## v1.0.1 (2025-05-19)

### Feat

- pass arbitrary keyword arguments to pandera scheme function

## v1.0.0 (2024-09-15)

### Feat

- add `--yes` flag for automatically confirmation

### Fix

- correct default arguments for included and excluded keys to empty list
- use python 3.8 compatible type annotations
- update pandera to a version supporting NumPy 2

## v1.0.0rc5 (2024-09-13)

### Feat

- allows to provide returned keys for custom data loaders

### Fix

- dynamically retrieve version from package meta data

## v1.0.0-rc4 (2023-06-14)

## v1.0.0-rc3 (2023-03-21)

## v1.0.0-rc2 (2023-02-16)

## v1.0.0-rc1 (2023-02-13)

## v0.0.7 (2023-02-07)

## v0.0.6 (2022-12-16)

## v0.0.5 (2022-12-01)

## v0.0.4 (2022-12-01)

## v0.0.3 (2022-12-01)

## v0.0.2 (2022-11-29)
