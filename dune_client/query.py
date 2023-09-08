"""
Data Classes Representing a Dune Query
"""
from __future__ import annotations
import urllib.parse
from dataclasses import dataclass
from typing import Optional, List, Dict, Union, Any

from dune_client.types import QueryParameter


def parse_query_object_or_id(
    query: Union[QueryBase, str, int],
) -> tuple[dict[str, str] | None, int]:
    """
    Users are allowed to pass QueryBase or ID into some functions.
    This method handles both scenarios, returning a pair of the form (params, query_id)
    """
    if isinstance(query, QueryBase):
        params = {f"params.{p.key}": p.to_dict()["value"] for p in query.parameters()}
        query_id = query.query_id
    else:
        params = None
        query_id = int(query)
    return params, query_id


@dataclass
class QueryBase:
    """Basic data structure constituting a Dune Analytics Query."""

    query_id: int
    name: str = "unnamed"
    params: Optional[List[QueryParameter]] = None

    def base_url(self) -> str:
        """Returns a link to query results excluding fixed parameters"""
        return f"https://dune.com/queries/{self.query_id}"

    def parameters(self) -> List[QueryParameter]:
        """Non-null version of self.params"""
        return self.params or []

    def url(self) -> str:
        """Returns a parameterized link to the query"""
        # Include variable parameters in the URL so they are set
        params = "&".join([f"{p.key}={p.value}" for p in self.parameters()])
        if params:
            return "?".join(
                [self.base_url(), urllib.parse.quote_plus(params, safe="=&?")]
            )
        return self.base_url()

    def __hash__(self) -> int:
        """
        This contains the query ID and the values of relevant parameters.
        Thus, it is unique for caching purposes
        """
        return self.url().__hash__()

    def request_format(self) -> Dict[str, Union[Dict[str, str], str, None]]:
        """Transforms Query objects to params to pass in API"""
        return {
            "query_parameters": {p.key: p.to_dict()["value"] for p in self.parameters()}
        }


@dataclass
class QueryMeta:  # pylint: disable=too-many-instance-attributes
    """
    Data class containing meta content about the query
    """

    description: str
    tags: list[str]
    version: int
    engine: str
    is_private: bool
    is_archived: bool
    is_unsaved: bool
    owner: str


@dataclass
class DuneQuery:
    """
    Enriched class representing all data constituting a DuneQuery
    Modeling the CRUD operation response for `get_query`
    """

    base: QueryBase
    meta: QueryMeta
    sql: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DuneQuery:
        """Constructor from json object"""
        return cls(
            base=QueryBase(
                query_id=int(data["query_id"]),
                name=data["name"],
                params=[
                    QueryParameter.from_dict(param) for param in data["parameters"]
                ],
            ),
            meta=QueryMeta(
                description=data["description"],
                tags=data["tags"],
                version=data["version"],
                engine=data["query_engine"],
                is_private=data["is_private"],
                is_archived=data["is_archived"],
                is_unsaved=data["is_unsaved"],
                owner=data["owner"],
            ),
            sql=data["query_sql"],
        )
