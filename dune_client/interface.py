"""
Abstract class for a basic Dune Interface with refresh method used by Query Runner.
"""
import abc

from dune_client.models import ResultsResponse
from dune_client.query import Query


# pylint: disable=too-few-public-methods
class DuneInterface(abc.ABC):
    """
    User Facing Methods for a Dune Client
    """

    @abc.abstractmethod
    def refresh(self, query: Query) -> ResultsResponse:
        """
        Executes a Dune query, waits till query execution completes,
        fetches and returns the results.
        """
