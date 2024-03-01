"""
Implementation of all Dune API query execution and get results routes.

Further Documentation:
    execution: https://dune.com/docs/api/api-reference/execute-queries/
    get results: https://dune.com/docs/api/api-reference/get-results/
"""

from io import BytesIO
from typing import Any, Dict, List, Optional, Union

from deprecated import deprecated

from dune_client.api.base import (
    BaseRouter,
    DUNE_CSV_NEXT_URI_HEADER,
    DUNE_CSV_NEXT_OFFSET_HEADER,
)
from dune_client.models import (
    ExecutionResponse,
    ExecutionStatusResponse,
    ResultsResponse,
    ExecutionResultCSV,
    DuneError,
)
from dune_client.query import QueryBase


class ExecutionAPI(BaseRouter):
    """
    Query execution and result fetching functions.
    """

    def execute_query(
        self, query: QueryBase, performance: Optional[str] = None
    ) -> ExecutionResponse:
        """Post's to Dune API for execute `query`"""
        params = query.request_format()
        params["performance"] = performance or self.performance

        self.logger.info(
            f"executing {query.query_id} on {performance or self.performance} cluster"
        )
        response_json = self._post(
            route=f"/query/{query.query_id}/execute",
            params=params,
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
            return success
        except KeyError as err:
            raise DuneError(response_json, "CancellationResponse", err) from err

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
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ResultsResponse:
        """GET results from Dune API for `job_id` (aka `execution_id`)"""
        params = self._build_parameters(
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
        )

        route = f"/execution/{job_id}/results"
        url = self._route_url(route)
        return self._get_execution_results_by_url(url=url, params=params)

    def get_execution_results_csv(
        self,
        job_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[str] = None,
        sample_count: Optional[int] = None,
        sort_by: Optional[List[str]] = None,
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
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> ResultsResponse:
        """
        GET results from Dune API with a given URL. This is particularly useful for pagination.
        """
        assert url.startswith(self.base_url)

        response_json = self._get(url=url, params=params)
        try:
            return ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    def _get_execution_results_csv_by_url(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
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

    ############
    # Utilities:
    ############

    def _build_parameters(
        self,
        params: Optional[Dict[str, Union[str, int]]] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Dict[str, Union[str, int]]:
        # Ensure we don't specify parameters that are incompatible:
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (limit is None and offset is None and filters is None)
        ), "sampling cannot be combined with filters or pagination"

        params = params or {}
        if columns is not None and len(columns) > 0:
            output = []
            for column in columns:
                # Escape all quotes and add quotes around it
                col = '"' + column.replace('"', '\\"') + '"'
                output.append(col)

            params["columns"] = ",".join(output)
        if sample_count is not None:
            params["sample_count"] = sample_count
        if filters is not None:
            params["filters"] = filters
        if sort_by is not None and len(sort_by) > 0:
            params["sort_by"] = ",".join(sort_by)
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        return params

    #######################
    # Deprecated Functions:
    #######################
    @deprecated(version="1.2.1", reason="Please use execute_query")
    def execute(
        self, query: QueryBase, performance: Optional[str] = None
    ) -> ExecutionResponse:
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
