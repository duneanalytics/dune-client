"""
Abstract class for a basic Dune Interface with refresh method used by Query Runner.
"""
from abc import ABC
from typing import List

from dune_client.query import Query
from dune_client.types import DuneRecord


# pylint: disable=too-few-public-methods
class DuneInterface(ABC):
    """
    User Facing Methods for a Dune Client
    """

    def refresh(self, query: Query) -> List[DuneRecord]:
        """
        Executes a Dune query, waits till query execution completes,
        fetches and returns the results.
        """
