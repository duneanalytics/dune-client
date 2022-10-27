"""
Abstract class for a basic Dune Interface with refresh method used by Query Runner.
"""
from abc import ABC

from dune_client.models import ResultsResponse
from dune_client.query import Query


# pylint: disable=too-few-public-methods
class DuneInterface(ABC):
    """
    User Facing Methods for a Dune Client
    """

    def refresh(self, query: Query) -> ResultsResponse:
        """
        Executes a Dune query, waits till query execution completes,
        fetches and returns the results.
        """
