[![img](https://img.shields.io/github/contributors/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/graphs/contributors)
[![img](https://img.shields.io/github/forks/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/network/members)
[![img](https://img.shields.io/github/stars/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/stargazers)
[![img](https://img.shields.io/github/issues/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/issues)
[![img](https://img.shields.io/github/license/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/blob/main/LICENSE)
[![img](https://img.shields.io/github/actions/workflow/status/MArpogaus/dvc-stage/test.yaml.svg?label=test&style=flat-square)](https://github.com/MArpogaus/dvc-stage/actions/workflows/test.yaml)
[![img](https://img.shields.io/github/actions/workflow/status/MArpogaus/dvc-stage/release.yaml.svg?label=release&style=flat-square)](https://github.com/MArpogaus/dvc-stage/actions/workflows/release.yaml)
[![img](https://img.shields.io/badge/pre--commit-enabled-brightgreen.svg?logo=pre-commit&style=flat-square)](https://github.com/MArpogaus/dvc-stage/blob/main/.pre-commit-config.yaml)
[![img](https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555)](https://linkedin.com/in/MArpogaus)

[![img](https://img.shields.io/pypi/v/dvc-stage.svg?style=flat-square)](https://pypi.org/project/dvc-stage)


# DVC-Stage

1.  [About The Project](#org2ce5616)
2.  [Getting Started](#orgd5c9d2a)
    1.  [Prerequisites](#orgb6c61f1)
    2.  [Installation](#org4a4a796)
3.  [Usage](#org7523cf7)
4.  [Contributing](#org06cba36)
5.  [License](#org88720e8)
6.  [Contact](#orgbac8fb8)
7.  [Acknowledgments](#orgd27aafc)


<a id="org2ce5616"></a>

## About The Project

This python script provides a easy and parameterizeable way of defining typical dvc (sub-)stages for:

-   data prepossessing
-   data transformation
-   data splitting
-   data validation


<a id="orgd5c9d2a"></a>

## Getting Started

This is an example of how you may give instructions on setting up your
project locally. To get a local copy up and running follow these simple
example steps.


<a id="orgb6c61f1"></a>

### Prerequisites

-   `pandas>=0.20.*`
-   `dvc>=2.12.*`
-   `pyyaml>=5`


<a id="org4a4a796"></a>

### Installation

This package is available on [PyPI](https://pypi.org/project/dvc-stage/).
You install it and all of its dependencies using pip:

    pip install dvc-stage


<a id="org7523cf7"></a>

## Usage

DVC-Stage works ontop of two files: `dvc.yaml` and `params.yaml`. They
are expected to be at the root of an initialized [dvc
project](https://dvc.org/). From there you can execute `dvc-stage -h` to see available
commands or `dvc-stage get-config STAGE` to generate the dvc stages from
the `params.yaml` file. The tool then generates the respective yaml
which you can then manually paste into the `dvc.yaml` file. Existing
stages can then be updated inplace using `dvc-stage update-stage STAGE`.

Stages are defined inside `params.yaml` in the following schema:

    STAGE_NAME:
      load: {}
      transformations: []
      validations: []
      write: {}

The `load` and `write` sections both require the yaml-keys `path` and
`format` to read and save data respectively.

The `transformations` and `validations` sections require a sequence of
functions to apply, where `transformations` return data and
`validations` return a truth value (derived from data). Functions are
defined by the key `id` an can be either:

-   Methods defined on Pandas Dataframes, e.g.

        transformations:
          - id: transpose

-   Imported from any python module, e.g.

        transformations:
          - id: custom
            description: duplikate rows
            import_from: demo.duplicate

-   Predefined by DVC-Stage, e.g.

        validations:
          - id: validate_pandera_schema
            schema:
              import_from: demo.get_schema

When writing a custom function, you need to make sure the function
gracefully handles data being `None`, which is required for type
inference. Data is passed as first argument. Further arguments can be
provided as additional keys, as shown above for
`validate_pandera_schema`, where schema is passed as second argument to
the function.

A working demonstration can be found at `examples/`.


<a id="org06cba36"></a>

## Contributing

Any Contributions are greatly appreciated! If you have a question, an issue or would like to contribute, please read our [contributing guidelines](CONTRIBUTING.md).


<a id="org88720e8"></a>

## License

Distributed under the [GNU General Public License v3](COPYING)


<a id="orgbac8fb8"></a>

## Contact

[Marcel Arpogaus](https://github.com/MArpogaus/) - [znepry.necbtnhf@tznvy.pbz](mailto:znepry.necbtnhf@tznvy.pbz) (encrypted with [ROT13](<https://rot13.com/>))

Project Link:
<https://github.com/MArpogaus/dvc-stage>


<a id="orgd27aafc"></a>

## Acknowledgments

Parts of this work have been funded by the Federal Ministry for the Environment, Nature Conservation and Nuclear Safety due to a decision of the German Federal Parliament (AI4Grids: 67KI2012A).
