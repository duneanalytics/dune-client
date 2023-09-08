"""
Implementation of all Dune API query execution and get results routes.

Further Documentation:
    execution: https://dune.com/docs/api/api-reference/execute-queries/
    get results: https://dune.com/docs/api/api-reference/get-results/
"""
from io import BytesIO
from typing import Optional

from deprecated import deprecated

from dune_client.api.base import BaseRouter
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

    def get_execution_results(self, job_id: str) -> ResultsResponse:
        """GET results from Dune API for `job_id` (aka `execution_id`)"""
        response_json = self._get(route=f"/execution/{job_id}/results")
        try:
            return ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    def get_execution_results_csv(self, job_id: str) -> ExecutionResultCSV:
        """
        GET results in CSV format from Dune API for `job_id` (aka `execution_id`)

        this API only returns the raw data in CSV format, it is faster & lighterweight
        use this method for large results where you want lower CPU and memory overhead
        if you need metadata information use get_results() or get_status()
        """
        route = f"/execution/{job_id}/results/csv"
        response = self._get(route=route, raw=True)
        response.raise_for_status()
        return ExecutionResultCSV(data=BytesIO(response.content))

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
