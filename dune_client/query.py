"""
Data Class Representing a Dune Query
"""
from dataclasses import dataclass
from typing import Optional

from duneapi.types import QueryParameter


@dataclass
class Query:
    """Basic data structure constituting a Dune Analytics Query."""

    name: str
    query_id: int
    params: Optional[list[QueryParameter]] = None

    def url(self) -> str:
        """Returns a link to query results excluding fixed parameters"""
        return f"https://dune.com/queries/{self.query_id}"

    def parameters(self) -> list[QueryParameter]:
        """Non-null version of self.params"""
        return self.params or []
