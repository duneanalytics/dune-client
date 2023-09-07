[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3102/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build](https://github.com/duneanalytics/dune-client/actions/workflows/pull-request.yaml/badge.svg)](https://github.com/duneanalytics/dune-client/actions/workflows/pull-request.yml)

# Dune Client

A python framework for interacting with Dune Analytics' [officially supported API
service](https://duneanalytics.notion.site/API-Documentation-1b93d16e0fa941398e15047f643e003a).

## Installation

Import as a project dependency

```shell
pip install dune-client
```

# Example Usage

Export your `DUNE_API_KEY` (or place it in a `.env` file - as in
here [.env.sample](./.env.sample) and `source .env`).

```python
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

query = QueryBase(
    name="Sample Query",
    query_id=1215383,
    params=[
        QueryParameter.text_type(name="TextField", value="Word"),
        QueryParameter.number_type(name="NumberField", value=3.1415926535),
        QueryParameter.date_type(name="DateField", value="2022-05-04 00:00:00"),
        QueryParameter.enum_type(name="EnumField", value="Option 1"),
    ],
)
print("Results available at", query.url())

dune = DuneClient.from_env()
results = dune.refresh(query)
```

For a more elaborate example,
visit [dune-alerts](https://github.com/cowprotocol/dune-alerts)

# Developer Usage & Deployment

## Makefile
This project's makefile comes equipt with sufficient commands for local development.

### Installation

```shell
make install
````

### Format, Lint & Types
```shell
make check
```
can also be run individually with `fmt`, `lint` and `types` respectively. 

### Testing
```shell
make test-unit  # Unit tests 
make test-e2e   # Requires valid `DUNE_API_KEY`
```
can also run both with `make test-all`

## Deployment

Publishing releases to PyPi is configured automatically via github actions 
(cf. [./.github/workflows/py-publish.yaml](./.github/workflows/py-publish.yaml)).
Any time a branch is tagged for release this workflow is triggered and published with the same version name.
