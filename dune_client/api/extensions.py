"""
Extended functionality for the ExecutionAPI
"""
from __future__ import annotations

import logging
import time

from io import BytesIO
from typing import Union, Optional, Any

from deprecated import deprecated

from dune_client.api.execution import ExecutionAPI
from dune_client.api.query import QueryAPI
from dune_client.models import (
    ResultsResponse,
    DuneError,
    ExecutionState,
    QueryFailed,
    ExecutionResultCSV,
)
from dune_client.query import QueryBase, parse_query_object_or_id
from dune_client.types import QueryParameter
from dune_client.util import age_in_hours

# This is the expiry time on old query results.
THREE_MONTHS_IN_HOURS = 2191
# Seconds between checking execution status
POLL_FREQUENCY_SECONDS = 1


class ExtendedAPI(ExecutionAPI, QueryAPI):
    """
    Provides higher level helper methods for faster
    and easier development on top of the base ExecutionAPI.
    """

    def run_query(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        performance: Optional[str] = None,
    ) -> ResultsResponse:
        """
        Executes a Dune `query`, waits until execution completes,
        fetches and returns the results.
        Sleeps `ping_frequency` seconds between each status request.
        """
        job_id = self._refresh(query, ping_frequency, performance)
        return self.get_execution_results(job_id)

    def run_query_csv(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        performance: Optional[str] = None,
    ) -> ExecutionResultCSV:
        """
        Executes a Dune query, waits till execution completes,
        fetches and the results in CSV format
        (use it load the data directly in pandas.from_csv() or similar frameworks)
        """
        job_id = self._refresh(query, ping_frequency, performance)
        return self.get_execution_results_csv(job_id)

    def run_query_dataframe(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        performance: Optional[str] = None,
    ) -> Any:
        """
        Execute a Dune Query, waits till execution completes,
        fetched and returns the result as a Pandas DataFrame

        This is a convenience method that uses run_query_csv() + pandas.read_csv() underneath
        """
        try:
            import pandas  # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise ImportError(
                "dependency failure, pandas is required but missing"
            ) from exc
        data = self.run_query_csv(query, ping_frequency, performance).data
        return pandas.read_csv(data)

    def get_latest_result(
        self,
        query: Union[QueryBase, str, int],
        max_age_hours: int = THREE_MONTHS_IN_HOURS,
    ) -> ResultsResponse:
        """
        GET the latest results for a query_id without re-executing the query
        (doesn't use execution credits)

        :param query: :class:`Query` object OR query id as string or int
        :param max_age_hours: re-executes the query if result is older than max_age_hours
            https://dune.com/docs/api/api-reference/get-results/latest-results
        """
        params, query_id = parse_query_object_or_id(query)
        response_json = self._get(
            route=f"/query/{query_id}/results",
            params=params,
        )
        try:
            results = ResultsResponse.from_dict(response_json)
            last_run = results.times.execution_ended_at
            if last_run and age_in_hours(last_run) > max_age_hours:
                # Query older than specified max age
                logging.info(
                    f"results (from {last_run}) older than {max_age_hours} hours, re-running query"
                )
                results = self.run_query(
                    query if isinstance(query, QueryBase) else QueryBase(query_id)
                )
            return results
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    def get_latest_result_dataframe(
        self,
        query: Union[QueryBase, str, int],
    ) -> Any:
        """
        GET the latest results for a query_id without re-executing the query
        (doesn't use execution credits)
        returns the result as a Pandas DataFrame

        This is a convenience method that uses get_latest_result() + pandas.read_csv() underneath
        """
        try:
            import pandas  # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise ImportError(
                "dependency failure, pandas is required but missing"
            ) from exc

        data = self.download_csv(query).data
        return pandas.read_csv(data)

    def download_csv(self, query: Union[QueryBase, str, int]) -> ExecutionResultCSV:
        """
        Almost like an alias for `get_latest_result` but for the csv endpoint.
        https://dune.com/docs/api/api-reference/get-results/latest-results
        """
        params, query_id = parse_query_object_or_id(query)
        response = self._get(
            route=f"/query/{query_id}/results/csv", params=params, raw=True
        )
        response.raise_for_status()
        return ExecutionResultCSV(data=BytesIO(response.content))

    ############################
    # Plus Subscription Features
    ############################
    def upload_csv(
        self,
        table_name: str,
        data: str,
        description: str = "",
        is_private: bool = False,
    ) -> bool:
        """
        https://dune.com/docs/api/api-reference/upload-data/?h=data+upload#endpoint
        The write API allows you to upload any .csv file into Dune. The only limitations are:

        - File has to be < 200 MB
        - Column names in the table can't start with a special character or digits.
        - Private uploads require a Premium subscription.

        Below are the specifics of how to work with the API.
        """
        response_json = self._post(
            route="/table/upload/csv",
            params={
                "table_name": table_name,
                "description": description,
                "data": data,
                "is_private": is_private,
            },
        )
        try:
            return bool(response_json["success"])
        except KeyError as err:
            raise DuneError(response_json, "UploadCsvResponse", err) from err

    ##############################################################################################
    # Premium Features: these features use APIs that are only available on paid subscription plans
    ##############################################################################################

    def run_sql(
        self,
        query_sql: str,
        params: Optional[list[QueryParameter]] = None,
        is_private: bool = True,
        archive_after: bool = True,
        performance: Optional[str] = None,
        name: str = "API Query",
    ) -> ResultsResponse:
        """
        Allows user to provide execute raw_sql via the CRUD interface
        - create, run, get results with optional archive/delete.
        - Query is by default made private and archived after execution.
        Requires premium subscription!
        """
        query = self.create_query(name, query_sql, params, is_private)
        results = self.run_query(query=query.base, performance=performance)
        if archive_after:
            self.archive_query(query.base.query_id)
        return results

    ######################
    # Deprecated Functions
    ######################
    @deprecated(version="1.2.1", reason="Please use run_query")
    def refresh(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
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
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
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
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        performance: Optional[str] = None,
    ) -> Any:
        """
        Execute a Dune Query, waits till execution completes,
        fetched and returns the result as a Pandas DataFrame

        This is a convenience method that uses refresh_csv underneath
        """
        return self.run_query_dataframe(query, ping_frequency, performance)

    #################
    # Private Methods
    #################
    def _refresh(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
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
            raise QueryFailed(f"Error data: {status.error}")

        return job_id
