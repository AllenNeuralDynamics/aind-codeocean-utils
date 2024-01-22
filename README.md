# aind-codeocean-utils

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-100.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?logo=codecov)
![Python](https://img.shields.io/badge/python->=3.7-blue?logo=python)

Library to contain useful utility methods to interface with Code Ocean.

## Installation

To use the package, you can install it from `pypi`:
```bash
pip install aind-codeocean-utils
```


To install the package from source, in the root directory, run
```bash
pip install -e .
```

To develop the code, run
```bash
pip install -e .[dev]
```

## Usage

The package includes helper functions to interact with Code Ocean:

### `CodeOceanJob`

This class enables one to run a job that:

1. Registers a new asset to Code Ocean from s3
2. Runs a capsule/pipeline on the newly registered asset (or an existing assey)
3. Captures the run results into a new asset

Steps 1 and 3 are optional, while step 2 (running the computation) is mandatory.

Here is a full example that registers a new ecephys asset, runs the spike sorting
capsule with some parameters, and registers the results:

```python
import os

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_utils.codeocean_job import (
    CodeOceanJob, CodeOceanJobConfig
)

# Set up the CodeOceanClient from aind_codeocean_api
CO_TOKEN = os.environ["CO_TOKEN"]
CO_DOMAIN = os.environ["CO_DOMAIN"]

co_client = CodeOceanClient(domain=CO_DOMAIN, token=CO_TOKEN)

# Define Job Parameters
job_config_dict = dict(
    register_config = dict(
        asset_name="test_dataset_for_codeocean_job",
        mount="ecephys_701305_2023-12-26_12-22-25",
        bucket="aind-ephys-data",
        prefix="ecephys_701305_2023-12-26_12-22-25",
        tags=["codeocean_job_test", "ecephys", "701305", "raw"],
        custom_metadata={
            "modality": "extracellular electrophysiology",
            "data level": "raw data",
        },
        viewable_to_everyone=True
    ),
    run_capsule_config = dict(
        data_assets=None, # when None, the newly registered asset will be used
        capsule_id="a31e6c81-49a5-4f1c-b89c-2d47ae3e02b4",
        run_parameters=["--debug", "--no-remove-out-channels"]
    ),
    capture_result_config = dict(
        process_name="sorted",
        tags=["np-ultra"] # additional tags to the ones inherited from input
    )
)

# instantiate config model
job_config = CodeOceanJobConfig(**job_config_dict)

# instantiate code ocean job
co_job = CodeOceanJob(co_client=co_client, job_config=job_config)

# run and wait for results
job_response = co_job.run_job()
```

This job will:
1. Register the `test_dataset_for_codeocean_job` asset from the specified s3 bucket and prefix
2. Run the capsule `a31e6c81-49a5-4f1c-b89c-2d47ae3e02b4` with the specified parameters
3. Register the result as `test_dataset_for_codeocean_job_sorter_{date-time}`


To run a computation on existing data assets, do not provide the `register_config` and
provide the `data_asset` field in the `run_capsule_config`.

To skip capturing the result, do not provide the `capture_result_config` option.


## Contributing

### Linters and testing

There are several libraries used to run linters, check documentation, and run tests.

- Please test your changes using the **coverage** library, which will run the tests and log a coverage report:

```bash
coverage run -m unittest discover && coverage report
```

- Use **interrogate** to check that modules, methods, etc. have been documented thoroughly:

```bash
interrogate .
```

- Use **flake8** to check that code is up to standards (no unused imports, etc.):
```bash
flake8 .
```

- Use **black** to automatically format the code into PEP standards:
```bash
black .
```

- Use **isort** to automatically sort import statements:
```bash
isort .
```

### Pull requests

For internal members, please create a branch. For external members, please fork the repository and open a pull request from the fork. We'll primarily use [Angular](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit) style for commit messages. Roughly, they should follow the pattern:
```text
<type>(<scope>): <short summary>
```

where scope (optional) describes the packages affected by the code changes and type (mandatory) is one of:

- **build**: Changes that affect build tools or external dependencies (example scopes: pyproject.toml, setup.py)
- **ci**: Changes to our CI configuration files and scripts (examples: .github/workflows/ci.yml)
- **docs**: Documentation only changes
- **feat**: A new feature
- **fix**: A bugfix
- **perf**: A code change that improves performance
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **test**: Adding missing tests or correcting existing tests

### Semantic Release

The table below, from [semantic release](https://github.com/semantic-release/semantic-release), shows which commit message gets you which release type when `semantic-release` runs (using the default configuration):

| Commit message                                                                                                                                                                                   | Release type                                                                                                    |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `fix(pencil): stop graphite breaking when too much pressure applied`                                                                                                                             | ~~Patch~~ Fix Release, Default release                                                                          |
| `feat(pencil): add 'graphiteWidth' option`                                                                                                                                                       | ~~Minor~~ Feature Release                                                                                       |
| `perf(pencil): remove graphiteWidth option`<br><br>`BREAKING CHANGE: The graphiteWidth option has been removed.`<br>`The default graphite width of 10mm is always used for performance reasons.` | ~~Major~~ Breaking Release <br /> (Note that the `BREAKING CHANGE: ` token must be in the footer of the commit) |

### Documentation
To generate the rst files source files for documentation, run
```bash
sphinx-apidoc -o doc_template/source/ src 
```
Then to create the documentation HTML files, run
```bash
sphinx-build -b html doc_template/source/ doc_template/build/html
```
More info on sphinx installation can be found [here](https://www.sphinx-doc.org/en/master/usage/installation.html).
