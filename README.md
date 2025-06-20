[![img](https://img.shields.io/github/contributors/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/graphs/contributors) [![img](https://img.shields.io/github/forks/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/network/members) [![img](https://img.shields.io/github/stars/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/stargazers) [![img](https://img.shields.io/github/issues/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/issues) [![img](https://img.shields.io/github/license/MArpogaus/dvc-stage.svg?style=flat-square)](https://github.com/MArpogaus/dvc-stage/blob/main/LICENSE) [![img](https://img.shields.io/github/actions/workflow/status/MArpogaus/dvc-stage/run_demo.yaml.svg?label=test&style=flat-square)](https://github.com/MArpogaus/dvc-stage/actions/workflows/run_demo.yaml) [![img](https://img.shields.io/github/actions/workflow/status/MArpogaus/dvc-stage/release.yaml.svg?label=release&style=flat-square)](https://github.com/MArpogaus/dvc-stage/actions/workflows/release.yaml) [![img](https://img.shields.io/badge/pre--commit-enabled-brightgreen.svg?logo=pre-commit&style=flat-square)](https://github.com/MArpogaus/dvc-stage/blob/main/.pre-commit-config.yaml) [![img](https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555)](https://linkedin.com/in/MArpogaus)

[![img](https://img.shields.io/pypi/v/dvc-stage.svg?style=flat-square)](https://pypi.org/project/dvc-stage)


# DVC-Stage

1.  [About The Project](#org0b9f792)
2.  [Getting Started](#org2a5d3ea)
    1.  [Prerequisites](#org4190b97)
    2.  [Installation](#orge4cf093)
3.  [Usage](#org57e65e6)
    1.  [Basic Stage Structure](#org425891e)
    2.  [Examples](#orgdca970f)
    3.  [Built-in Transformations](#org32fae61)
    4.  [Built-in Validations](#orgb49453a)
    5.  [Using Custom Functions](#org391efe7)
4.  [Contributing](#org82ce8b3)
5.  [License](#org1dfb3ef)
6.  [Contact](#org80d0f10)
7.  [Acknowledgments](#org70aa47e)


<a id="org0b9f792"></a>

## About The Project

This python script provides a easy and parameterizeable way of defining typical dvc (sub-)stages for:

-   data prepossessing
-   data transformation
-   data splitting
-   data validation


<a id="org2a5d3ea"></a>

## Getting Started

This is an example of how you may give instructions on setting up your project locally. To get a local copy up and running follow these simple example steps.


<a id="org4190b97"></a>

### Prerequisites

-   `pandas>=0.20.*`
-   `dvc>=2.12.*`
-   `pyyaml>=5`


<a id="orge4cf093"></a>

### Installation

This package is available on [PyPI](https://pypi.org/project/dvc-stage/). You install it and all of its dependencies using pip:

```bash
pip install dvc-stage
```


<a id="org57e65e6"></a>

## Usage

DVC-Stage works on top of two files: `dvc.yaml` and `params.yaml`. They are expected to be at the root of an initialized [dvc project](https://dvc.org/). From there you can execute `dvc-stage -h` to see available commands or `dvc-stage get-config STAGE` to generate the dvc stages from the `params.yaml` file. The tool then generates the respective yaml which you can then manually paste into the `dvc.yaml` file. Existing stages can then be updated inplace using `dvc-stage update-stage STAGE`.


<a id="org425891e"></a>

### Basic Stage Structure

Stages are defined inside `params.yaml` in the following schema:

```yaml
STAGE_NAME:
  load: {}
  transformations: []
  validations: []
  write: {}
```

The `load` and `write` sections both require the yaml-keys `path` and `format` to read and save data respectively.

The `transformations` and `validations` sections require a sequence of functions to apply, where `transformations` return data and `validations` return a truth value (derived from data). Functions are defined by the key `id` and can be either:

-   Methods defined on Pandas DataFrames, e.g.

    ```yaml
    transformations:
      - id: transpose
    ```

-   Imported from any python module, e.g.

    ```yaml
    transformations:
      - id: custom
        description: duplicate rows
        import_from: demo.duplicate
    ```

-   Predefined by DVC-Stage, e.g.

    ```yaml
    validations:
      - id: validate_pandera_schema
        schema:
          import_from: demo.get_schema
    ```

When writing a custom function, you need to make sure the function gracefully handles data being `None`, which is required for type inference. Data is passed as first argument. Further arguments can be provided as additional keys, as shown above for `validate_pandera_schema`, where schema is passed as second argument to the function.


<a id="orgdca970f"></a>

### Examples

The `examples` directory contains a complete working demonstration:

1.  **Setup**: Navigate to the examples directory
2.  **Data**: Sample data files are provided in `data`
3.  **Configuration**: `params.yaml` contains all pipeline definitions
4.  **Custom functions**: `src/demo.py` contains example custom functions
5.  **DVC configuration**: `dvc.yaml` contains the generated DVC stages

To run all examples:

```bash
cd examples

# Update all stage deffinitions
dvc-stage update-all -y

# Reproduce pipeline
dvc repro
```

1.  Example 1: Basic Demo Pipeline

    The simplest example demonstrates basic data loading, transformation, validation, and writing:

    ```yaml
    demo_pipeline:
      dvc_stage_args:
        log-level: ${log_level}
        log-file: ${log_file}
      load:
        path: load.csv
        format: csv
      transformations:
      - id: custom
        description: duplicate rows
        import_from: demo.duplicate
      - id: transpose
      - id: rename
        columns:
          0.0: O1
          1.0: O2
          2.0: D1
          3.0: D2
      validations:
      - id: custom
        description: check none
        import_from: demo.isNotNone
      - id: isnull
        reduction: any
        expected: false
      - id: validate_pandera_schema
        schema:
          import_from: demo.get_schema
      write:
        format: csv
    ```

    **What this pipeline does:**

    1.  **Load**: Reads data from `load.csv`
    2.  **Transform**:
        -   Duplicates all rows using a custom function
        -   Transposes the DataFrame
        -   Renames columns from numeric to meaningful names
    3.  **Validate**:
        -   Checks that data is not None
        -   Ensures no null values exist
        -   Validates against a Pandera schema
    4.  **Write**: Saves the result to `outdir/out.csv`

    **Run with:**

    ```bash
    cd examples
    dvc repro demo_pipeline
    ```

2.  Example 2: Foreach Pipeline

    Process multiple datasets with the same pipeline using foreach stages:

    ```yaml
    foreach_pipeline:
      dvc_stage_args:
        log-level: ${log_level}
        log-file: ${log_file}
      foreach: [dataset_a, dataset_b, dataset_c]
      load:
        path: data/${item}/input.csv
        format: csv
      transformations:
      - id: fillna
        value: 0
      - id: custom
        description: normalize data
        import_from: demo.normalize_data
        columns: [value1, value2]
      validations:
      - id: validate_pandera_schema
        schema:
          import_from: demo.get_foreach_schema
      - id: custom
        description: check data quality
        import_from: demo.check_data_quality
        min_rows: 5
      write:
        path: outdir/${item}_${key}_processed.csv
    ```

    **What this pipeline does:**

    1.  **Foreach**: Processes three datasets (dataset<sub>a</sub>, dataset<sub>b</sub>, dataset<sub>c</sub>)
    2.  **Load**: Reads from `data/${item}/input.csv` where `${item}` is replaced with each dataset name
    3.  **Transform**:
        -   Fills missing values with 0
        -   Normalizes specified columns using min-max scaling
    4.  **Validate**:
        -   Validates against a pandera schema
        -   Checks data quality (minimum row count)
    5.  **Write**: Saves each processed dataset to `outdir/${item}_${key}_processed.csv`

    **Run with:**

    ```bash
    cd examples
    dvc repro foreach_pipeline
    ```

3.  Example 3: Advanced Multi-Input Pipeline

    Handle multiple input files with data splitting:

    ```yaml
    advanced_pipeline:
      dvc_stage_args:
        log-level: ${log_level}
        log-file: ${log_file}
      load:
        path:
        - data/features.csv
        - data/labels.csv
        format: csv
        key_map:
          features: data/features.csv
          labels: data/labels.csv
      transformations:
      - id: split
        include: [features]
        by: id
        id_col: category
        left_split_key: train
        right_split_key: test
        size: 0.5
        seed: 42
      - id: combine
        include: [train, test]
        new_key: combined_data
      validations:
      - id: validate_pandera_schema
        schema:
          import_from: demo.get_advanced_schema
        include: [combined]
      write:
        path: outdir/${key}.csv
    ```

    **What this pipeline does:**

    1.  **Load**: Reads multiple files and maps them to keys (features, labels)
    2.  **Transform**:
        -   The features table is spitted along the categories in two data frames containing each 50% of the data
        -   The spitted data is again combined into a single table
    3.  **Validate**: Validates both train and test sets against a schema
    4.  **Write**: Saves train.csv and test.csv to the output directory

    **Run with:**

    ```bash
    cd examples
    dvc repro advanced_pipeline
    ```

4.  Example 4: Time Series Pipeline

    Process time series data with date-based splitting:

    ```yaml
    timeseries_pipeline:
      dvc_stage_args:
        log-level: ${log_level}
        log-file: ${log_file}
      load:
        path: data/timeseries.csv
        format: csv
        parse_dates: [timestamp]
        index_col: timestamp
      transformations:
      - id: reset_index
      - id: add_date_offset_to_column
        column: timestamp
        days: 1
      - id: split
        by: date_time
        left_split_key: train
        right_split_key: test
        size: 0.8
        freq: D
        date_time_col: timestamp
      - id: set_index
        keys: timestamp
      validations:
      - id: validate_pandera_schema
        schema:
          import_from: demo.get_timeseries_schema
      - id: custom
        description: validate split ratio
        pass_dict_to_fn: true
        import_from: demo.validate_split_ratio
        reduction: none
        expected_ratio: 0.8
        tolerance: 0.05
      write:
        path: outdir/timeseries_${key}.csv
    ```

    **What this pipeline does:**

    1.  **Load**: Reads time series data with proper datetime parsing
    2.  **Transform**:
        -   Reset pandas index
        -   Adds a date offset to the timestamps
        -   Splits data chronologically (80% train, 20% test) by date
        -   Set timestamp as index
    3.  **Validate**:
        -   Validates against a time series specific schema
        -   Validate the split ratio
    4.  **Write**: Saves timeseries<sub>train.csv</sub> and timeseries<sub>test.csv</sub>

    **Run with:**

    ```bash
    cd examples
    dvc repro timeseries_pipeline
    ```


<a id="org32fae61"></a>

### Built-in Transformations

DVC-Stage provides several built-in transformations:

-   **split**: Split data (random, date<sub>time</sub>, or id-based)
-   **combine**: Combine multiple DataFrames
-   **column<sub>transformer</sub><sub>fit</sub>**: Fit sklearn column transformers
-   **column<sub>transformer</sub><sub>transform</sub>**: Apply fitted transformers
-   **add<sub>date</sub><sub>offset</sub><sub>to</sub><sub>column</sub>**: Add time offsets to date columns

    Additionally all pandas DataFrame methods can be used, e.g.:

-   **fillna**: Fill missing values
-   **dropna**: Drop rows with missing values
-   **transpose**: Transpose the DataFrame
-   **rename**: Rename columns


<a id="orgb49453a"></a>

### Built-in Validations

DVC-Stage provides several built-in validations:

-   **validate<sub>pandera</sub><sub>schema</sub>**: Validate against Pandera schemas
-   **Custom validations**: Import your own validation functions

Additionally all pandas DataFrame methods can be used, e.g.:

-   **isnull**: Check for null values


<a id="org391efe7"></a>

### Using Custom Functions

When creating custom functions for transformations or validations:

1.  **Handle None gracefully**: Your function should return appropriate values when data is None
2.  **First argument is data**: The DataFrame or data structure is always the first parameter
3.  **Additional parameters**: Pass extra arguments as YAML keys in your stage definition
4.  **Return appropriate types**: Transformations return data, validations return boolean values

Example custom function:

```python
def normalize_data(data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Normalize specified columns using min-max scaling."""
    if data is None:
        return None

    result = data.copy()
    for col in columns:
        if col in result.columns:
            min_val = result[col].min()
            max_val = result[col].max()
            if max_val > min_val:
                result[col] = (result[col] - min_val) / (max_val - min_val)
    return result
```


<a id="org82ce8b3"></a>

## Contributing

Any Contributions are greatly appreciated! If you have a question, an issue or would like to contribute, please read our [contributing guidelines](CONTRIBUTING.md).


<a id="org1dfb3ef"></a>

## License

Distributed under the [GNU General Public License v3](COPYING)


<a id="org80d0f10"></a>

## Contact

[Marcel Arpogaus](https://github.com/MArpogaus/) - [znepry.necbtnhf@tznvy.pbz](mailto:znepry.necbtnhf@tznvy.pbz) (encrypted with [ROT13](<https://rot13.com/>))

Project Link: <https://github.com/MArpogaus/dvc-stage>


<a id="org70aa47e"></a>

## Acknowledgments

Parts of this work have been funded by the Federal Ministry for the Environment, Nature Conservation and Nuclear Safety due to a decision of the German Federal Parliament (AI4Grids: 67KI2012A).
