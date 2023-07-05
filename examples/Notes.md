# Notes
- dvc init required
- pip install git+https://github.com/MArpogaus/dvc-stage.git@dev

` dvc-stage get-config demo_pipeline > dvc.yaml `

For writing stage, output path needs to be in a subdirectory!

Transformer function needs to be **installed** (reference by path does not work?!)
Id refers to the _kind_ of function (custom or internally provided signature).
The Function also **needs** to be able to gracefully handle None input. data is given as first parameter. Sibling keys to the immport_from key get passed as named arguments to the function.

Pandera import from yaml does not work (pandara[io] not installed allthough it is)
