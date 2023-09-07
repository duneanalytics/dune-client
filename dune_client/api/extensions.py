"""
Extended functionality for the ExecutionAPI
"""

import time
from typing import Union, Optional, Any

from deprecated import deprecated

from dune_client.api.execution import ExecutionAPI
from dune_client.models import (
    ResultsResponse,
    DuneError,
    ExecutionState,
    QueryFailed,
    ExecutionResultCSV,
)
from dune_client.query import QueryBase


class ExtendedAPI(ExecutionAPI):
    """
    Provides higher level helper methods for faster and easier development on top of the base ExecutionAPI.
    """

    def _refresh(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
        performance: Optional[str] = None,
    ) -> str:
        """
        Executes a Dune `query`, waits until execution completes,
        fetches and returns the results.
        Sleeps `ping_frequency` seconds between each status request.
        """
        job_id = self.execute_query(query=query, performance=performance).execution_id
        status = self.get_execution_status(job_id)
        while status.state not in ExecutionState.terminal_states():
            self.logger.info(
                f"waiting for query execution {job_id} to complete: {status}"
            )
            time.sleep(ping_frequency)
            status = self.get_execution_status(job_id)
        if status.state == ExecutionState.FAILED:
            self.logger.error(status)
            raise QueryFailed(f"{status}. Perhaps your query took too long to run!")

        return job_id

    def run_query(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
        performance: Optional[str] = None,
    ) -> ResultsResponse:
        """
        Executes a Dune `query`, waits until execution completes,
        fetches and returns the results.
        Sleeps `ping_frequency` seconds between each status request.
        """
        job_id = self._refresh(
            query, ping_frequency=ping_frequency, performance=performance
        )
        return self.get_execution_results(job_id)

    def run_query_csv(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
        performance: Optional[str] = None,
    ) -> ExecutionResultCSV:
        """
        Executes a Dune query, waits till execution completes,
        fetches and the results in CSV format
        (use it load the data directly in pandas.from_csv() or similar frameworks)
        """
        job_id = self._refresh(
            query, ping_frequency=ping_frequency, performance=performance
        )
        return self.get_execution_results_csv(job_id)

    def run_query_dataframe(
        self, query: QueryBase, performance: Optional[str] = None
    ) -> Any:
        """
        Execute a Dune Query, waits till execution completes,
        fetched and returns the result as a Pandas DataFrame

        This is a convenience method that uses run_query_csv() + pandas.read_csv() underneath
        """
        try:
            import pandas  # type: ignore # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise ImportError(
                "dependency failure, pandas is required but missing"
            ) from exc
        data = self.run_query_csv(query, performance=performance).data
        return pandas.read_csv(data)

    def get_latest_result(self, query: Union[QueryBase, str, int]) -> ResultsResponse:
        """
        GET the latest results for a query_id without re-executing the query (doesn't use execution credits)

        :param query: :class:`Query` object OR query id as string | int

        https://dune.com/docs/api/api-reference/latest_results/
        """
        if isinstance(query, QueryBase):
            params = {
                f"params.{p.key}": p.to_dict()["value"] for p in query.parameters()
            }
            query_id = query.query_id
        else:
            params = None
            query_id = int(query)

        response_json = self._get(
            route=f"/query/{query_id}/results",
            params=params,
        )
        try:
            return ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    # Deprecated Functions
    @deprecated(version="1.2.1", reason="Please use run_query")
    def refresh(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
        performance: Optional[str] = None,
    ) -> ResultsResponse:
        """
        Executes a Dune `query`, waits until execution completes,
        fetches and returns the results.
        Sleeps `ping_frequency` seconds between each status request.
        """
        return self.run_query(query, ping_frequency, performance)

    @deprecated(version="1.2.1", reason="Please use run_query_csv")
    def refresh_csv(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
        performance: Optional[str] = None,
    ) -> ExecutionResultCSV:
        """
        Executes a Dune query, waits till execution completes,
        fetches and the results in CSV format
        (use it load the data directly in pandas.from_csv() or similar frameworks)
        """
        return self.run_query_csv(query, ping_frequency, performance)

    @deprecated(version="1.2.1", reason="Please use run_query_dataframe")
    def refresh_into_dataframe(
        self, query: QueryBase, performance: Optional[str] = None
    ) -> Any:
        """
        Execute a Dune Query, waits till execution completes,
        fetched and returns the result as a Pandas DataFrame

        This is a convenience method that uses refresh_csv underneath
        """
        return self.run_query_dataframe(query, performance)

    def upload_csv(self, table_name: str, data: str, description: str = "") -> bool:
        """
        https://dune.com/docs/api/api-reference/upload-data/?h=data+upload#endpoint
        The write API allows you to upload any .csv file into Dune. The only limitations are:

        - File has to be < 200 MB
        - Column names in the table can't start with a special character or digits.

        Below are the specifics of how to work with the API.
        """
        response_json = self._post(
            route="/table/upload/csv",
            params={
                "table_name": table_name,
                "description": description,
                "data": data,
            },
        )
        try:
            return bool(response_json["success"])
        except KeyError as err:
            raise DuneError(response_json, "upload_csv response", err) from err
