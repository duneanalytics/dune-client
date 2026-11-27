[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build](https://github.com/duneanalytics/dune-client/actions/workflows/pull-request.yaml/badge.svg)](https://github.com/duneanalytics/dune-client/actions/workflows/pull-request.yaml)

# Dune Client

A python framework for interacting with Dune Analytics' [officially supported API
service](https://docs.dune.com/api-reference/overview/introduction).

## Installation

### Using uv (recommended)

[uv](https://docs.astral.sh/uv/) is a fast Python package and project manager written in Rust. It provides:
- ⚡ 10-100x faster dependency resolution
- 🔒 Reproducible builds with lockfiles
- 🐍 Automatic Python version management
- 📦 Zero-configuration virtual environments

Install uv first, then:

```shell
# Add to an existing project
uv add dune-client

# Or create a new project
uv init my-dune-project
cd my-dune-project
uv add dune-client
```

### Using pip

```shell
pip install dune-client
```

# Example Usage

## Quickstart: run_query

Export your `DUNE_API_KEY` before running these examples. The client reads it from the environment.

> 💡 **Tip**: If using uv, you can run examples directly without activating a virtual environment:
> ```shell
> uv run python your_script.py
> ```

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
        QueryParameter.enum_type(name="ListField", value="Option 1"),
    ],
)
print("Results available at", query.url())

dune = DuneClient()
results = dune.run_query(query)
print(results.get_rows())

# or as CSV
# results_csv = dune.run_query_csv(query)

# or as Pandas Dataframe
# results_df = dune.run_query_dataframe(query)
```

## Further Examples

### Get Latest Results
`get_latest_result` fetches the most recent result, but re-executes the query if it is older
than `max_age_hours`. The default threshold is about three months. A re-execution uses
execution credits; fetching a result that is still fresh does not. Use `download_csv(query_id)`
instead if you need the latest existing CSV result without triggering a new execution.

```python
from dune_client.client import DuneClient

dune = DuneClient()
results = dune.get_latest_result(1215383, max_age_hours=8)
print(results.get_rows())
```

### Async Query Execution

Use the async client as a context manager so it closes its HTTP session when finished:

```python
import asyncio

from dune_client.client_async import AsyncDuneClient
from dune_client.query import QueryBase


async def main():
    async with AsyncDuneClient() as dune:
        results = await dune.run_query(QueryBase(query_id=1215383))
        print(results.get_rows())


asyncio.run(main())
```

### Discover Datasets

List datasets by owner, then inspect the columns of one dataset:

```python
from dune_client.client import DuneClient

dune = DuneClient()
page = dune.list_datasets(owner_handle="dune", limit=50)
for dataset in page.datasets:
    details = dune.get_dataset(dataset.full_name)
    print(details.full_name, [column.name for column in details.columns])
```

Use `offset` with `list_datasets` to fetch later pages.

### Submit and Track Contracts

Submitting a contract requires a Read/Write API key. Replace the address and ABI with your
contract's details. The idempotency key makes retries safe:

```python
import json

from dune_client.client import DuneClient
from dune_client.models import ContractSubmissionInput

with open("my_contract.abi.json", encoding="utf-8") as file:
    abi = json.load(file)

dune = DuneClient()
submission = ContractSubmissionInput(
    blockchain_name="ethereum",
    address="0xYOUR_CONTRACT_ADDRESS",
    project_name="my_project",
    contract_name="MyContract",
    abi=abi,
    idempotency_key="my-project/my-contract/v1",
)
for result in dune.submit_contracts([submission]).results:
    print(result.index, result.submission_id, result.error)

pending = dune.list_contract_submissions(status="pending")
print([item.id for item in pending.submissions])
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
    SELECT block_time, hash
    FROM ethereum.transactions
    ORDER BY CAST(gas_used as uint256) * CAST(gas_price AS uint256) DESC
    LIMIT {{N}}
    """

dune = DuneClient()
query = dune.create_query(
    name="Top {N} Most Expensive Transactions on Ethereum",
    query_sql=sql,
    # Optional fields
    params=[QueryParameter.number_type(name="N", value=10)],
    is_private=False,  # default
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
    tags=["XP€N$IV $H1T"],
)

dune.archive_query(query_id)
dune.unarchive_query(query_id)

dune.make_private(query_id)
dune.make_public(query_id)
```

# Developer Usage & Deployment

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and development workflows.

## Setup
```shell
# Clone the repository
git clone https://github.com/duneanalytics/dune-client.git
cd dune-client

# Install dependencies
uv sync --dev
```

## Development Commands
```shell
# Format code
uv run ruff format

# Lint code
uv run ruff check

# Type checking
uv run mypy dune_client/ --strict

# Run tests
uv run python -m pytest tests/unit      # Unit tests
uv run python -m pytest tests/e2e       # E2E tests (requires DUNE_API_KEY)
```

## Multi-Python Testing

This project supports Python 3.11, 3.12, and 3.13. You can test across all versions using tox with uv:

```shell
# Test all Python versions with tox
uv run tox
# or
make test-tox

# Test specific Python versions
uv run tox -e py311
uv run tox -e py312
uv run tox -e py313
```

## Makefile Shortcuts

### Installation
```shell
make install        # Uses uv sync
# or
make install-dev    # Uses uv sync --dev
```

### Format, Lint & Types
```shell
make check
```
can also be run individually with `fmt`, `lint` and `types` respectively.

### Testing
```shell
make test-unit  # Unit tests
make test-e2e   # Requires valid `DUNE_API_KEY`
make test-all   # Both unit and e2e tests
make test-tox   # Multi-Python testing (py311, py312, py313)
```

## Deployment

Publishing releases to PyPi is configured automatically via github actions
(cf. [./.github/workflows/py-publish.yaml](./.github/workflows/py-publish.yaml)).
Any time a branch is tagged for release this workflow is triggered and published with the same version name.
