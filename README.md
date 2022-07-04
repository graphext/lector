# Lector

## Install

### From Github

Simply run

```
pip install git+https://github.com/graphext/lector
```

### Development

To install a local copy for development, including all dependencies for test, documentation and code quality, use the following commands:

``` bash
clone git+https://github.com/graphext/lector
cd lector
pip install -v -e ".[dev]"
pre-commit install
```

The [pre-commit](https://pre-commit.com/) command will make sure that whenever you try to commit changes to this repo code quality and formatting tools will be executed. This ensures e.g. a common coding style, such that any changes to be commited are functional changes only, not changes due to different personal coding style preferences. This in turn makes it either to collaborate via pull requests etc.

To test installation you may execute the [pytest](https://docs.pytest.org/) suite to make sure everything's setup correctly, e.g.:

``` bash
pytest -v .
```

## Command line interface

## Notebook

If you installed this packages into a brand new environment, and depending on the kind of environment (venv, conda etc.), after installation you may have to register this environment with jupyter for it to show as a kernel in notebooks:

``` bash
ipython kernel install --name [myenv] --user
```

Following this, start jupyter with `jupyter notebook` and it should let you select the kernel containing your lector installation.

## ToDo

### Parallelize type inference

Good test cases:

- l***y_bc2_random_cl_1M.csv: 1k columns of ints (2GB on disk) take 176 sec, and uses only a small fraction of 1 CPU.
- midyear_population_age_country_code.csv
