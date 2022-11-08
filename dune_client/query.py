"""
Data Class Representing a Dune Query
"""
import urllib.parse
from dataclasses import dataclass
from typing import Optional, List, Dict

from dune_client.types import QueryParameter


@dataclass
class Query:
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

    def request_format(self) -> Dict[str, Dict[str, str]]:
        """Transforms Query objects to params to pass in API"""
        return {
            "query_parameters": {p.key: p.to_dict()["value"] for p in self.parameters()}
        }
