"""
Implementation of all Dune API query execution and get results routes.

Further Documentation:
    execution: https://docs.dune.com/api-reference/executions/endpoint/execute-query
    get results: https://docs.dune.com/api-reference/executions/endpoint/get-execution-result
"""

from __future__ import annotations

from io import BytesIO
from typing import Any

from deprecated import deprecated

from dune_client.api.base import (
    DUNE_CSV_NEXT_OFFSET_HEADER,
    DUNE_CSV_NEXT_URI_HEADER,
    BaseRouter,
)
from dune_client.models import (
    DuneError,
    ExecutionResponse,
    ExecutionResultCSV,
    ExecutionState,
    ExecutionStatusResponse,
    PipelineExecutionResponse,
    ResultsResponse,
)
from dune_client.query import QueryBase  # noqa: TC001


class ExecutionAPI(BaseRouter):
    """
    Query execution and result fetching functions.
    """

    def execute_query(self, query: QueryBase, performance: str | None = None) -> ExecutionResponse:
        """Post's to Dune API for execute `query`"""
        params = query.request_format()
        params["performance"] = performance or self.performance

        self.logger.info(f"executing {query.query_id} on {performance or self.performance} cluster")
        response_json = self._post(
            route=f"/query/{query.query_id}/execute",
            params=params,
        )
        try:
            return ExecutionResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ExecutionResponse", err) from err

    def execute_query_pipeline(
        self, query_id: int, performance: str | None = None
    ) -> PipelineExecutionResponse:
        """Post's to Dune API for execute query pipeline"""
        params: dict[str, str] = {}
        if performance is not None:
            params["performance"] = performance

        self.logger.info(f"executing pipeline for query {query_id}")
        response_json = self._post(
            route=f"/query/{query_id}/pipeline/execute",
            params=params,
        )
        try:
            return PipelineExecutionResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "PipelineExecutionResponse", err) from err

    def execute_sql(
        self,
        query_sql: str,
        performance: str | None = None,
    ) -> ExecutionResponse:
        """
        Execute arbitrary SQL directly via the API without creating a saved query.
        https://docs.dune.com/api-reference/executions/endpoint/execute-sql

        Note: This endpoint does not support parameterized queries. If you need
        parameters, use the regular execute_query() with a saved query.

        Args:
            query_sql: The SQL query string to execute
            performance: Optional performance tier ("medium" or "large")

        Returns:
            ExecutionResponse with execution_id and state
        """
        payload: dict[str, str] = {
            "sql": query_sql,
            "performance": performance or self.performance,
        }

        self.logger.info(f"executing SQL on {performance or self.performance} cluster")
        response_json = self._post(
            route="/sql/execute",
            params=payload,
        )
        try:
            return ExecutionResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ExecutionResponse", err) from err

    def cancel_execution(self, job_id: str) -> bool:
        """POST Execution Cancellation to Dune API for `job_id` (aka `execution_id`)"""
        response_json = self._post(
            route=f"/execution/{job_id}/cancel",
            params=None,
        )
        try:
            # No need to make a dataclass for this since it's just a boolean.
            success: bool = response_json["success"]
        except KeyError as err:
            raise DuneError(response_json, "CancellationResponse", err) from err
        else:
            return success

    def get_execution_status(self, job_id: str) -> ExecutionStatusResponse:
        """GET status from Dune API for `job_id` (aka `execution_id`)"""
        response_json = self._get(route=f"/execution/{job_id}/status")
        try:
            return ExecutionStatusResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ExecutionStatusResponse", err) from err

    def get_execution_results(
        self,
        job_id: str,
        limit: int | None = None,
        offset: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
        allow_partial_results: str = "true",
    ) -> ResultsResponse:
        """GET results from Dune API for `job_id` (aka `execution_id`)"""
        params = self._build_parameters(
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
            allow_partial_results=allow_partial_results,
        )

        route = f"/execution/{job_id}/results"
        url = self._route_url(route)
        return self._get_execution_results_by_url(url=url, params=params)

    def get_execution_results_csv(
        self,
        job_id: str,
        limit: int | None = None,
        offset: int | None = None,
        columns: list[str] | None = None,
        filters: str | None = None,
        sample_count: int | None = None,
        sort_by: list[str] | None = None,
    ) -> ExecutionResultCSV:
        """
        GET results in CSV format from Dune API for `job_id` (aka `execution_id`)

        this API only returns the raw data in CSV format, it is faster & lighterweight
        use this method for large results where you want lower CPU and memory overhead
        if you need metadata information use get_results() or get_status()
        """
        params = self._build_parameters(
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
        )

        route = f"/execution/{job_id}/results/csv"
        url = self._route_url(route)
        return self._get_execution_results_csv_by_url(url=url, params=params)

    def _get_execution_results_by_url(
        self, url: str, params: dict[str, Any] | None = None
    ) -> ResultsResponse:
        """
        GET results from Dune API with a given URL. This is particularly useful for pagination.
        """
        assert url.startswith(self.base_url)

        response_json = self._get(url=url, params=params)
        try:
            result = ResultsResponse.from_dict(response_json)
            if result.state == ExecutionState.PARTIAL:
                self.logger.warning(
                    f"execution {result.execution_id} resulted in a partial "
                    f"result set (i.e. results too large)."
                )
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err
        else:
            return result

    def _get_execution_results_csv_by_url(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> ExecutionResultCSV:
        """
        GET results in CSV format from Dune API with a given URL. This is particularly
        useful for pagination

        this API only returns the raw data in CSV format, it is faster & lighterweight
        use this method for large results where you want lower CPU and memory overhead
        if you need metadata information use get_results() or get_status()
        """
        assert url.startswith(self.base_url)

        response = self._get(url=url, params=params, raw=True)
        response.raise_for_status()
        next_uri = response.headers.get(DUNE_CSV_NEXT_URI_HEADER)
        next_offset = response.headers.get(DUNE_CSV_NEXT_OFFSET_HEADER)
        return ExecutionResultCSV(
            data=BytesIO(response.content),
            next_uri=next_uri,
            next_offset=next_offset,
        )

    #######################
    # Deprecated Functions:
    #######################
    @deprecated(version="1.2.1", reason="Please use execute_query")
    def execute(self, query: QueryBase, performance: str | None = None) -> ExecutionResponse:
        """Post's to Dune API for execute `query`"""
        return self.execute_query(query, performance)

    @deprecated(version="1.2.1", reason="Please use get_execution_status")
    def get_status(self, job_id: str) -> ExecutionStatusResponse:
        """GET status from Dune API for `job_id` (aka `execution_id`)"""
        return self.get_execution_status(job_id)

    @deprecated(version="1.2.1", reason="Please use get_execution_results")
    def get_result(self, job_id: str) -> ResultsResponse:
        """GET results from Dune API for `job_id` (aka `execution_id`)"""
        return self.get_execution_results(job_id)

    @deprecated(version="1.2.1", reason="Please use get_execution_results_csv")
    def get_result_csv(self, job_id: str) -> ExecutionResultCSV:
        """
        GET results in CSV format from Dune API for `job_id` (aka `execution_id`)

        this API only returns the raw data in CSV format, it is faster & lighterweight
        use this method for large results where you want lower CPU and memory overhead
        if you need metadata information use get_results() or get_status()
        """
        return self.get_execution_results_csv(job_id)
