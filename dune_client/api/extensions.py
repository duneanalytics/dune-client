"""
Extended functionality for the ExecutionAPI
"""

from __future__ import annotations

import logging
import time

from io import BytesIO
from typing import Any, List, Optional, Union

from deprecated import deprecated

from dune_client.api.base import (
    DUNE_CSV_NEXT_URI_HEADER,
    DUNE_CSV_NEXT_OFFSET_HEADER,
    MAX_NUM_ROWS_PER_BATCH,
)
from dune_client.api.execution import ExecutionAPI
from dune_client.api.query import QueryAPI
from dune_client.api.table import TableAPI
from dune_client.api.custom import CustomEndpointAPI
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


class ExtendedAPI(ExecutionAPI, QueryAPI, TableAPI, CustomEndpointAPI):
    """
    Provides higher level helper methods for faster
    and easier development on top of the base ExecutionAPI.
    """

    def run_query(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        performance: Optional[str] = None,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
        allow_partial_results: str = "true",
    ) -> ResultsResponse:
        """
        Executes a Dune `query`, waits until execution completes,
        fetches and returns the results.
        Sleeps `ping_frequency` seconds between each status request.
        """
        # Ensure we don't specify parameters that are incompatible:
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (batch_size is None and filters is None)
        ), "sampling cannot be combined with filters or pagination"

        if sample_count is not None:
            limit = None
        else:
            limit = batch_size or MAX_NUM_ROWS_PER_BATCH

        # pylint: disable=duplicate-code
        job_id = self._refresh(query, ping_frequency, performance)
        return self._fetch_entire_result(
            self.get_execution_results(
                job_id,
                columns=columns,
                sample_count=sample_count,
                filters=filters,
                sort_by=sort_by,
                limit=limit,
                allow_partial_results=allow_partial_results,
            ),
        )

    def run_query_csv(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        performance: Optional[str] = None,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ExecutionResultCSV:
        """
        Executes a Dune query, waits till execution completes,
        fetches and the results in CSV format
        (use it load the data directly in pandas.from_csv() or similar frameworks)
        """
        # Ensure we don't specify parameters that are incompatible:
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (batch_size is None and filters is None)
        ), "sampling cannot be combined with filters or pagination"

        if sample_count is not None:
            limit = None
        else:
            limit = batch_size or MAX_NUM_ROWS_PER_BATCH

        # pylint: disable=duplicate-code
        job_id = self._refresh(query, ping_frequency, performance)
        return self._fetch_entire_result_csv(
            self.get_execution_results_csv(
                job_id,
                columns=columns,
                sample_count=sample_count,
                filters=filters,
                sort_by=sort_by,
                limit=limit,
            ),
        )

    def run_query_dataframe(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        performance: Optional[str] = None,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
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
        data = self.run_query_csv(
            query,
            ping_frequency,
            performance,
            batch_size=batch_size,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
        ).data
        return pandas.read_csv(data)

    def get_latest_result(
        self,
        query: Union[QueryBase, str, int],
        max_age_hours: int = THREE_MONTHS_IN_HOURS,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ResultsResponse:
        """
        GET the latest results for a query_id without re-executing the query
        (doesn't use execution credits)

        :param query: :class:`Query` object OR query id as string or int
        :param max_age_hours: re-executes the query if result is older than max_age_hours
            https://docs.dune.com/api-reference/executions/endpoint/get-query-result
        """
        # Ensure we don't specify parameters that are incompatible:
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (batch_size is None and filters is None)
        ), "sampling cannot be combined with filters or pagination"

        params, query_id = parse_query_object_or_id(query)

        # Only fetch 1 row to get metadata first to determine if the result is fresh enough
        if params is None:
            params = {}
        params["limit"] = 1

        response_json = self._get(
            route=f"/query/{query_id}/results",
            params=params,
        )
        try:
            if sample_count is None and batch_size is None:
                batch_size = MAX_NUM_ROWS_PER_BATCH
            metadata = ResultsResponse.from_dict(response_json)
            last_run = metadata.times.execution_ended_at

            if last_run and age_in_hours(last_run) > max_age_hours:
                # Query older than specified max age, we need to refresh the results
                logging.info(
                    f"results (from {last_run}) older than {max_age_hours} hours, re-running query"
                )
                results = self.run_query(
                    query if isinstance(query, QueryBase) else QueryBase(query_id),
                    columns=columns,
                    sample_count=sample_count,
                    filters=filters,
                    sort_by=sort_by,
                    batch_size=batch_size,
                )
            else:
                # The results are fresh enough, retrieve the entire result
                # pylint: disable=duplicate-code
                results = self._fetch_entire_result(
                    self.get_execution_results(
                        metadata.execution_id,
                        columns=columns,
                        sample_count=sample_count,
                        filters=filters,
                        sort_by=sort_by,
                        limit=batch_size,
                    ),
                )
            return results
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    def get_latest_result_dataframe(
        self,
        query: Union[QueryBase, str, int],
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
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

        results = self.download_csv(
            query,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            batch_size=batch_size,
        )
        return pandas.read_csv(results.data)

    def download_csv(
        self,
        query: Union[QueryBase, str, int],
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ExecutionResultCSV:
        """
        Almost like an alias for `get_latest_result` but for the csv endpoint.
        https://docs.dune.com/api-reference/executions/endpoint/get-query-result-csv
        """
        # Ensure we don't specify parameters that are incompatible:
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (batch_size is None and filters is None)
        ), "sampling cannot be combined with filters or pagination"

        params, query_id = parse_query_object_or_id(query)

        params = self._build_parameters(
            params=params,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=batch_size,
        )
        if sample_count is None and batch_size is None:
            params["limit"] = MAX_NUM_ROWS_PER_BATCH

        response = self._get(
            route=f"/query/{query_id}/results/csv", params=params, raw=True
        )
        response.raise_for_status()

        next_uri = response.headers.get(DUNE_CSV_NEXT_URI_HEADER)
        next_offset = response.headers.get(DUNE_CSV_NEXT_OFFSET_HEADER)
        return self._fetch_entire_result_csv(
            ExecutionResultCSV(
                data=BytesIO(response.content),
                next_uri=next_uri,
                next_offset=next_offset,
            ),
        )

    ##############################################################################################
    # Plus Features: these features use APIs that are only available on paid subscription plans
    ##############################################################################################

    def run_sql(
        self,
        query_sql: str,
        params: Optional[list[QueryParameter]] = None,
        is_private: bool = True,
        archive_after: bool = True,
        performance: Optional[str] = None,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        name: str = "API Query",
    ) -> ResultsResponse:
        """
        Allows user to provide execute raw_sql via the CRUD interface
        - create, run, get results with optional archive/delete.
        - Query is by default made private and archived after execution.
        Requires Plus subscription!
        """
        query = self.create_query(name, query_sql, params, is_private)
        try:
            results = self.run_query(
                query=query.base, performance=performance, ping_frequency=ping_frequency
            )
        finally:
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
        if status.state == ExecutionState.PENDING:
            self.logger.warning("Partial result set retrieved.")
        if status.state == ExecutionState.FAILED:
            self.logger.error(status)
            raise QueryFailed(f"Error data: {status.error}")
        return job_id

    def _fetch_entire_result(
        self,
        results: ResultsResponse,
    ) -> ResultsResponse:
        """
        Retrieve the entire results using the paginated API
        """
        next_uri = results.next_uri
        while next_uri is not None:
            batch = self._get_execution_results_by_url(url=next_uri)
            results += batch
            next_uri = batch.next_uri

        return results

    def _fetch_entire_result_csv(
        self,
        results: ExecutionResultCSV,
    ) -> ExecutionResultCSV:
        """
        Retrieve the entire results in CSV format using the paginated API
        """
        next_uri = results.next_uri
        while next_uri is not None:
            batch = self._get_execution_results_csv_by_url(url=next_uri)
            results += batch
            next_uri = batch.next_uri

        return results
