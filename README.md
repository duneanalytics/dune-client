[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3102/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build](https://github.com/duneanalytics/dune-client/actions/workflows/pull-request.yaml/badge.svg)](https://github.com/duneanalytics/dune-client/actions/workflows/pull-request.yaml)

# Dune Client

A python framework for interacting with Dune Analytics' [officially supported API
service](https://duneanalytics.notion.site/API-Documentation-1b93d16e0fa941398e15047f643e003a).

## Installation

Import as a project dependency

```shell
pip install dune-client
```

# Example Usage

## Quickstart: run_query

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
results = dune.run_query(query)

# or as CSV
# results_csv = dune.run_query_csv(query)

# or as Pandas Dataframe
# results_df = dune.run_query_dataframe(query)
```

## Further Examples

### Get Latest Results
Use `get_latest_results` to get the most recent query results without using execution credits. 
You can specify a `max_age_hours` to re-run the query if the data is too outdated.

```python
from dune_client.client import DuneClient

dune = DuneClient.from_env()
results = dune.get_latest_result(1215383, max_age_hours=8)
```

## Paid Subscription Features

### CRUD Operations

If you're writing scripts that rely on Dune query results and want to ensure that your local, 
peer-reviewed, queries are being used at runtime, you can call `update_query` before `run_query`!

Here is a fictitious example making use of this functionality;

```python
from dune_client.types import QueryParameter
from dune_client.client import DuneClient

sql = """
    SELECT block_time, hash,
    FROM ethereum.transactions
    ORDER BY CAST(gas_used as uint256) * CAST(gas_price AS uint256) DESC
    LIMIT {{N}}
    """

dune = DuneClient.from_env()
query = dune.create_query(
    name="Top {N} Most Expensive Transactions on Ethereum",
    query_sql=sql,
    # Optional fields
    params=[QueryParameter.number_type(name="N", value=10)],
    is_private=False  # default
)
query_id = query.base.query_id
print(f"Created query with id {query.base.query_id}")
# Could retrieve using 
# dune.get_query(query_id)

dune.update_query(
    query_id, 
    # All parameters below are optional
    name="Top {N} Most Expensive Transactions on {Blockchain}",
    query_sql=sql.replace("ethereum", "{{Blockchain}}"),
    params=query.base.parameters() + [QueryParameter.text_type("Blockchain", "ethereum")],
    description="Shows time and hash of the most expensive transactions",
    tags=["XP€N$IV $H1T"]
)

dune.archive_query(query_id)
dune.unarchive_query(query_id)

dune.make_private(query_id)
dune.make_public(query_id)
```

# Developer Usage & Deployment

## Makefile
This project's makefile comes equipped with sufficient commands for local development.

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
