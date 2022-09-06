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
here [.env.sample](./.env.sample).

```python
import dotenv
import os

from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import Query

query = Query(
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

dotenv.load_dotenv()
dune = DuneClient(os.environ["DUNE_API_KEY"])
results = dune.refresh(query)
```

For a more elaborate example,
visit [dune-alerts](https://github.com/cowprotocol/dune-alerts)

