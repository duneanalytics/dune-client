"""
Extended functionality for the ExecutionAPI
"""

from __future__ import annotations

import time
from io import BytesIO
from typing import TYPE_CHECKING, Any

from deprecated import deprecated

from dune_client.api.base import (
    DUNE_CSV_NEXT_OFFSET_HEADER,
    DUNE_CSV_NEXT_URI_HEADER,
    MAX_NUM_ROWS_PER_BATCH,
)
from dune_client.api.custom import CustomEndpointAPI
from dune_client.api.datasets import DatasetsAPI
from dune_client.api.execution import ExecutionAPI
from dune_client.api.pipeline import PipelineAPI
from dune_client.api.query import QueryAPI
from dune_client.api.table import TableAPI
from dune_client.api.uploads import UploadsAPI
from dune_client.api.usage import UsageAPI
from dune_client.models import (
    DuneError,
    ExecutionResultCSV,
    ExecutionState,
    QueryFailedError,
    ResultsResponse,
)
from dune_client.query import QueryBase, parse_query_object_or_id
from dune_client.util import age_in_hours

if TYPE_CHECKING:
    from dune_client.types import QueryParameter

# This is the expiry time on old query results.
THREE_MONTHS_IN_HOURS = 2191
# Seconds between checking execution status
POLL_FREQUENCY_SECONDS = 1


class ExtendedAPI(  # type: ignore[misc]
    ExecutionAPI,
    QueryAPI,
    UploadsAPI,
    DatasetsAPI,
    TableAPI,
    UsageAPI,
    CustomEndpointAPI,
    PipelineAPI,
):
    """
    Provides higher level helper methods for faster
    and easier development on top of the base ExecutionAPI.

    Includes both legacy TableAPI (deprecated) and modern UploadsAPI/DatasetsAPI.
    UploadsAPI is listed before TableAPI in the MRO to ensure modern methods
    take precedence over deprecated ones with the same name.

    Note: TableAPI has incompatible method signatures with UploadsAPI but is
    kept for backward compatibility. The UploadsAPI methods take precedence.
    """

    def run_query(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        performance: str | None = None,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
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

        limit = None if sample_count is not None else batch_size or MAX_NUM_ROWS_PER_BATCH

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
        performance: str | None = None,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
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

        limit = None if sample_count is not None else batch_size or MAX_NUM_ROWS_PER_BATCH

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
        performance: str | None = None,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> Any:
        """
        Execute a Dune Query, waits till execution completes,
        fetched and returns the result as a Pandas DataFrame

        This is a convenience method that uses run_query_csv() + pandas.read_csv() underneath
        """
        try:
            import pandas as pd  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError("dependency failure, pandas is required but missing") from exc
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
        return pd.read_csv(data)

    def get_latest_result(
        self,
        query: QueryBase | str | int,
        max_age_hours: int = THREE_MONTHS_IN_HOURS,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
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
                self.logger.info(
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
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err
        else:
            return results

    def get_latest_result_dataframe(
        self,
        query: QueryBase | str | int,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> Any:
        """
        GET the latest results for a query_id without re-executing the query
        (doesn't use execution credits)
        returns the result as a Pandas DataFrame

        This is a convenience method that uses get_latest_result() + pandas.read_csv() underneath
        """
        try:
            import pandas as pd  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError("dependency failure, pandas is required but missing") from exc

        results = self.download_csv(
            query,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            batch_size=batch_size,
        )
        return pd.read_csv(results.data)

    def download_csv(
        self,
        query: QueryBase | str | int,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
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

        response = self._get(route=f"/query/{query_id}/results/csv", params=params, raw=True)
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
        params: list[QueryParameter] | None = None,
        is_private: bool = True,
        archive_after: bool = True,
        performance: str | None = None,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        name: str = "API Query",
    ) -> ResultsResponse:
        """
        Execute arbitrary SQL directly via the API and return results.
        Uses the /sql/execute endpoint introduced in the Dune API.
        https://docs.dune.com/api-reference/executions/endpoint/execute-query

        Note: The `name`, `is_private`, `archive_after`, and `params` parameters are
        kept for backward compatibility but are ignored when using the direct SQL execution
        endpoint. The /sql/execute endpoint does not support parameterized queries.

        Args:
            query_sql: The SQL query string to execute
            params: (Ignored) Kept for backward compatibility
            is_private: (Ignored) Kept for backward compatibility
            archive_after: (Ignored) Kept for backward compatibility
            performance: Optional performance tier ("medium" or "large")
            ping_frequency: Seconds between status checks while polling
            name: (Ignored) Kept for backward compatibility

        Returns:
            ResultsResponse with the query execution results

        Requires Plus subscription!
        """
        # Execute SQL directly using the new endpoint
        job_id = self.execute_sql(
            query_sql=query_sql,
            performance=performance,
        ).execution_id

        # Poll for completion
        status = self.get_execution_status(job_id)
        while status.state not in ExecutionState.terminal_states():
            self.logger.info(f"waiting for query execution {job_id} to complete: {status}")
            time.sleep(ping_frequency)
            status = self.get_execution_status(job_id)

        if status.state == ExecutionState.FAILED:
            self.logger.error(status)
            if status.error:
                raise QueryFailedError(status.error.message)
            raise QueryFailedError("Query execution failed")

        # Fetch and return results
        return self._fetch_entire_result(self.get_execution_results(job_id))

    ######################
    # Deprecated Functions
    ######################
    @deprecated(version="1.2.1", reason="Please use run_query")
    def refresh(
        self,
        query: QueryBase,
        ping_frequency: int = POLL_FREQUENCY_SECONDS,
        performance: str | None = None,
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
        performance: str | None = None,
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
        performance: str | None = None,
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
        performance: str | None = None,
    ) -> str:
        """
        Executes a Dune `query`, waits until execution completes,
        fetches and returns the results.
        Sleeps `ping_frequency` seconds between each status request.
        """
        job_id = self.execute_query(query=query, performance=performance).execution_id
        status = self.get_execution_status(job_id)
        while status.state not in ExecutionState.terminal_states():
            self.logger.info(f"waiting for query execution {job_id} to complete: {status}")
            time.sleep(ping_frequency)
            status = self.get_execution_status(job_id)
        if status.state == ExecutionState.PENDING:
            self.logger.warning("Partial result set retrieved.")
        if status.state == ExecutionState.FAILED:
            self.logger.error(status)
            if status.error:
                raise QueryFailedError(status.error.message)
            raise QueryFailedError("Query execution failed")
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
