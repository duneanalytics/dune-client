"""
Abstract class for a basic Dune Interface with refresh method used by Query Runner.
"""
import abc
from typing import Any

from dune_client.models import ResultsResponse, ExecutionResultCSV
from dune_client.query import QueryBase


# pylint: disable=too-few-public-methods
class DuneInterface(abc.ABC):
    """
    User Facing Methods for a Dune Client
    """

    @abc.abstractmethod
    def refresh(self, query: QueryBase) -> ResultsResponse:
        """
        Executes a Dune query, waits till query execution completes,
        fetches and returns the results.
        """

    @abc.abstractmethod
    def refresh_csv(self, query: QueryBase) -> ExecutionResultCSV:
        """
        Executes a Dune query, waits till execution completes,
        fetches and the results in CSV format
        (use it load the data directly in pandas.from_csv() or similar frameworks)

        this Dune API only returns the raw data in CSV format, it is faster & lighterweight
        use this method for large results where you want lower CPU and memory overhead
        """

    @abc.abstractmethod
    def refresh_into_dataframe(self, query: QueryBase) -> Any:
        """
        Execute a Dune Query, waits till execution completes,
        fetched and returns the result as a Pandas DataFrame

        This is a convenience method that uses refresh_csv underneath
        it assumes the caller has already called `import pandas`
        """
