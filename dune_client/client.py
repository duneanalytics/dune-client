""""
Basic Dune Client Class responsible for refreshing Dune Queries
Framework built on Dune's API Documentation
https://duneanalytics.notion.site/API-Documentation-1b93d16e0fa941398e15047f643e003a
"""
from __future__ import annotations

import time
from io import BytesIO
from typing import Any, Optional, Union

import requests
from deprecated import deprecated
from requests import Response, JSONDecodeError

from dune_client.base_client import BaseDuneClient
from dune_client.models import (
    ExecutionResponse,
    ExecutionResultCSV,
    DuneError,
    QueryFailed,
    ExecutionStatusResponse,
    ResultsResponse,
    ExecutionState,
)
from dune_client.query import QueryBase, DuneQuery
from dune_client.types import QueryParameter


class DuneClient(BaseDuneClient):  # pylint: disable=too-many-public-methods
    """
    An interface for Dune API with a few convenience methods
    combining the use of endpoints (e.g. refresh)
    """

    def _handle_response(self, response: Response) -> Any:
        """Generic response handler utilized by all Dune API routes"""
        try:
            # Some responses can be decoded and converted to DuneErrors
            response_json = response.json()
            self.logger.debug(f"received response {response_json}")
            return response_json
        except JSONDecodeError as err:
            # Others can't. Only raise HTTP error for not decodable errors
            response.raise_for_status()
            raise ValueError("Unreachable since previous line raises") from err

    def _route_url(self, route: str) -> str:
        return f"{self.BASE_URL}{self.api_version}{route}"

    def _get(
        self,
        route: str,
        params: Optional[Any] = None,
        raw: bool = False,
    ) -> Any:
        """Generic interface for the GET method of a Dune API request"""
        url = self._route_url(route)
        self.logger.debug(f"GET received input url={url}")
        response = requests.get(
            url=url,
            headers=self.default_headers(),
            timeout=self.DEFAULT_TIMEOUT,
            params=params,
        )
        if raw:
            return response
        return self._handle_response(response)

    def _post(self, route: str, params: Optional[Any] = None) -> Any:
        """Generic interface for the POST method of a Dune API request"""
        url = self._route_url(route)
        self.logger.debug(f"POST received input url={url}, params={params}")
        response = requests.post(
            url=url,
            json=params,
            headers=self.default_headers(),
            timeout=self.DEFAULT_TIMEOUT,
        )
        return self._handle_response(response)

    def _patch(self, route: str, params: Any) -> Any:
        """Generic interface for the PATCH method of a Dune API request"""
        url = self._route_url(route)
        self.logger.debug(f"PATCH received input url={url}, params={params}")
        response = requests.request(
            method="PATCH",
            url=url,
            json=params,
            headers={"x-dune-api-key": self.token},
            timeout=self.DEFAULT_TIMEOUT,
        )
        return self._handle_response(response)

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

    def get_latest_result(self, query: Union[QueryBase, str, int]) -> ResultsResponse:
        """
        GET the latest results for a query_id without having to execute the query again.

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

    # CRUD Operations: https://dune.com/docs/api/api-reference/edit-queries/
    def create_query(
        self,
        name: str,
        query_sql: str,
        params: Optional[list[QueryParameter]] = None,
        is_private: bool = False,
    ) -> DuneQuery:
        """
        Creates Dune Query by ID
        https://dune.com/docs/api/api-reference/edit-queries/create-query/
        """
        parameters = {
            "name": name,
            "query_sql": query_sql,
            "private": is_private,
        }
        if params is not None:
            parameters["parameters"] = [p.to_dict() for p in params]
        response_json = self._post(route="/query/", params=parameters)
        try:
            query_id = int(response_json["query_id"])
            # Note that this requires an extra request.
            return self.get_query(query_id)
        except KeyError as err:
            raise DuneError(response_json, "create_query Response", err) from err

    def get_query(self, query_id: int) -> DuneQuery:
        """
        Retrieves Dune Query by ID
        https://dune.com/docs/api/api-reference/edit-queries/get-query/
        """
        response_json = self._get(route=f"/query/{query_id}")
        return DuneQuery.from_dict(response_json)

    def update_query(  # pylint: disable=too-many-arguments
        self,
        query_id: int,
        name: Optional[str] = None,
        query_sql: Optional[str] = None,
        params: Optional[list[QueryParameter]] = None,
        description: Optional[str] = None,
        tags: Optional[list[str]] = None,
    ) -> int:
        """
        Updates Dune Query by ID
        https://dune.com/docs/api/api-reference/edit-queries/update-query

        The request body should contain all fields that need to be updated.
        Any omitted fields will be left untouched.
        If the tags or parameters are provided as an empty array,
        they will be deleted from the query.
        """
        parameters: dict[str, Any] = {}
        if name is not None:
            parameters["name"] = name
        if description is not None:
            parameters["description"] = description
        if tags is not None:
            parameters["tags"] = tags
        if query_sql is not None:
            parameters["query_sql"] = query_sql
        if params is not None:
            parameters["parameters"] = [p.to_dict() for p in params]

        if not bool(parameters):
            # Nothing to change no need to make reqeust
            self.logger.warning("called update_query with no proposed changes.")
            return query_id

        response_json = self._patch(
            route=f"/query/{query_id}",
            params=parameters,
        )
        try:
            # No need to make a dataclass for this since it's just a boolean.
            return int(response_json["query_id"])
        except KeyError as err:
            raise DuneError(response_json, "update_query Response", err) from err

    def archive_query(self, query_id: int) -> bool:
        """
        https://dune.com/docs/api/api-reference/edit-queries/archive-query
        returns resulting value of Query.is_archived
        """
        response_json = self._post(route=f"/query/{query_id}/archive")
        try:
            # No need to make a dataclass for this since it's just a boolean.
            return self.get_query(int(response_json["query_id"])).meta.is_archived
        except KeyError as err:
            raise DuneError(response_json, "make_private Response", err) from err

    def unarchive_query(self, query_id: int) -> bool:
        """
        https://dune.com/docs/api/api-reference/edit-queries/archive-query
        returns resulting value of Query.is_archived
        """
        response_json = self._post(route=f"/query/{query_id}/unarchive")
        try:
            # No need to make a dataclass for this since it's just a boolean.
            return self.get_query(int(response_json["query_id"])).meta.is_archived
        except KeyError as err:
            raise DuneError(response_json, "make_private Response", err) from err

    def make_private(self, query_id: int) -> None:
        """
        https://dune.com/docs/api/api-reference/edit-queries/private-query
        """
        response_json = self._post(route=f"/query/{query_id}/private")
        assert self.get_query(int(response_json["query_id"])).meta.is_private

    def make_public(self, query_id: int) -> None:
        """
        https://dune.com/docs/api/api-reference/edit-queries/private-query
        """
        response_json = self._post(route=f"/query/{query_id}/unprivate")
        assert not self.get_query(int(response_json["query_id"])).meta.is_private

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
        url = self._route_url(f"/execution/{job_id}/results/csv")
        self.logger.debug(f"GET CSV received input url={url}")
        response = self._get(route=route, raw=True)
        response.raise_for_status()
        return ExecutionResultCSV(data=BytesIO(response.content))

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

        This is a convenience method that uses refresh_csv underneath
        """
        try:
            import pandas  # type: ignore # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise ImportError(
                "dependency failure, pandas is required but missing"
            ) from exc
        data = self.run_query_csv(query, performance=performance).data
        return pandas.read_csv(data)
