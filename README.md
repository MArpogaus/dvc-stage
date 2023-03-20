[![img](https://img.shields.io/github/contributors/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/graphs/contributors)
[![img](https://img.shields.io/github/forks/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/network/members)
[![img](https://img.shields.io/github/stars/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/stargazers)
[![img](https://img.shields.io/github/issues/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/issues)
[![img](https://img.shields.io/github/license/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/blob/master/COPYING)
[![img](https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555)](https://linkedin.com/in/MArpogaus)


# DVC-Stage

1.  [About The Project](#about-the-project)
2.  [Getting Started](#getting-started)
    1.  [Prerequisites](#prerequisites)
    2.  [Installation](#installation)
3.  [Usage](#usage)
4.  [License](#license)
5.  [Contact](#contact)
6.  [Acknowledgments](#acknowledgments)


<a id="about-the-project"></a>

## About The Project

This python script provides a easy and parameterizeable way of defining typical dvc stages for:

-   data prepossessing
-   data transformation
-   data splitting
-   data validation


<a id="getting-started"></a>

## Getting Started

This is an example of how you may give instructions on setting up your
project locally. To get a local copy up and running follow these simple
example steps.


<a id="prerequisites"></a>

### Prerequisites

-   `pandas>=0.20.*`
-   `dvc>=2.12.*`
-   `pyyaml>=5`


<a id="installation"></a>

### Installation

    pip install git+https://github.com/MArpogaus/dvc-stage.git


<a id="usage"></a>

## Usage

DVC-Stage works ontop of two files: `dvc.yaml` and `params.yaml`.
They are expected to be at the root of an initialized [dvc project](https://dvc.org/).
From there you can execute `dvc-stage -h` to see available commands or `dvc-stage get-config STAGE` to generate the dvc stages from the `params.yaml` file. The tool then generates the respective yaml which you can then manually paste into the `dvc.yaml` file. Existing stages can then be updated inplace using `dvc-stage update-stage STAGE`.

Stages are defined inside `params.yaml` in the following schema:
```yaml
STAGE_NAME:
  load: {}
  transformations: []
  validations: []
  write: {}
```

The `load` and `write` sections both require the yaml-keys `path` and `format` to read and save data respectively.

The `transformations` and `validations` sections require a sequence of functions to apply, where `transformations` return data and `validations` return a truth value (derived from data). 
Functions are defined by the key `id` an can be either:
 - Methods defined on Pandas Dataframes, e.g. 
   ```yaml
   transformations:
   - id: transpose
   ```
 - Imported from any python module, e.g.
   ```yaml
   transformations:
   - id: custom
     description: duplikate rows
     import_from: demo.duplicate
   ```
 - Predefined by DVC-Stage, e.g.
   ```yaml
   validations:
   - id: validate_pandera_schema
     schema:
     import_from: demo.get_schema
   ```
   
When writing a custom function, you need to make sure the function gracefully handles data being `None`, which is required for type inference. Data is passed as first argument. Further arguments can be provided as additional keys, as shown above for `validate_pandera_schema`, where schema is passed as second argument to the function.


A working demonstration can be found at `examples/`.


<a id="license"></a>

## License

Distributed under the [GNU General Public License v3](COPYING)


<a id="contact"></a>

## Contact

[Marcel Arpogaus](https://github.com/MArpogaus/) - [marcel.arpogaus@gmail.com](mailto:marcel.arpogaus@gmail.com)

Project Link:
<https://github.com/MArpogaus/dvc-stage>


<a id="acknowledgments"></a>

## Acknowledgments

Parts of this work have been funded by the Federal Ministry for the Environment, Nature Conservation and Nuclear Safety due to a decision of the German Federal Parliament (AI4Grids: 67KI2012A).
