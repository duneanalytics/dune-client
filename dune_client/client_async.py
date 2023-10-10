""""
Async Dune Client Class responsible for refreshing Dune Queries
Framework built on Dune's API Documentation
https://duneanalytics.notion.site/API-Documentation-1b93d16e0fa941398e15047f643e003a
"""
from __future__ import annotations

import asyncio
from io import BytesIO
from typing import Any, Optional, Union

from aiohttp import (
    ClientSession,
    ClientResponse,
    ContentTypeError,
    TCPConnector,
    ClientTimeout,
)

from dune_client.api.base import BaseDuneClient
from dune_client.models import (
    ExecutionResponse,
    ExecutionResultCSV,
    DuneError,
    QueryFailed,
    ExecutionStatusResponse,
    ResultsResponse,
    ExecutionState,
)

from dune_client.query import QueryBase, parse_query_object_or_id


# pylint: disable=duplicate-code
class AsyncDuneClient(BaseDuneClient):
    """
    An asynchronous interface for Dune API with a few convenience methods
    combining the use of endpoints (e.g. refresh)
    """

    _connection_limit = 3

    def __init__(
        self, api_key: str, connection_limit: int = 3, performance: str = "medium"
    ):
        """
        api_key - Dune API key
        connection_limit - number of parallel requests to execute.
        For non-pro accounts Dune allows only up to 3 requests but that number can be increased.
        """
        super().__init__(api_key=api_key, performance=performance)
        self._connection_limit = connection_limit
        self._session: Optional[ClientSession] = None

    async def _create_session(self) -> ClientSession:
        conn = TCPConnector(limit=self._connection_limit)
        return ClientSession(
            connector=conn,
            base_url=self.base_url,
            timeout=ClientTimeout(total=self.request_timeout),
        )

    async def connect(self) -> None:
        """Opens a client session (can be used instead of async with)"""
        self._session = await self._create_session()

    async def disconnect(self) -> None:
        """Closes client session"""
        if self._session:
            await self._session.close()

    async def __aenter__(self) -> AsyncDuneClient:
        self._session = await self._create_session()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.disconnect()

    async def _handle_response(self, response: ClientResponse) -> Any:
        try:
            # Some responses can be decoded and converted to DuneErrors
            response_json = await response.json()
            self.logger.debug(f"received response {response_json}")
            return response_json
        except ContentTypeError as err:
            # Others can't. Only raise HTTP error for not decodable errors
            response.raise_for_status()
            raise ValueError("Unreachable since previous line raises") from err

    def _route_url(self, route: str) -> str:
        return f"{self.api_version}{route}"

    async def _get(
        self,
        route: str,
        params: Optional[Any] = None,
        raw: bool = False,
    ) -> Any:
        url = self._route_url(route)
        if self._session is None:
            raise ValueError("Client is not connected; call `await cl.connect()`")
        self.logger.debug(f"GET received input url={url}")
        response = await self._session.get(
            url=url,
            headers=self.default_headers(),
            params=params,
        )
        if raw:
            return response
        return await self._handle_response(response)

    async def _post(self, route: str, params: Any) -> Any:
        url = self._route_url(route)
        if self._session is None:
            raise ValueError("Client is not connected; call `await cl.connect()`")
        self.logger.debug(f"POST received input url={url}, params={params}")
        response = await self._session.post(
            url=url,
            json=params,
            headers=self.default_headers(),
        )
        return await self._handle_response(response)

    async def execute(
        self, query: QueryBase, performance: Optional[str] = None
    ) -> ExecutionResponse:
        """Post's to Dune API for execute `query`"""
        params = query.request_format()
        params["performance"] = performance or self.performance

        self.logger.info(
            f"executing {query.query_id} on {performance or self.performance} cluster"
        )
        response_json = await self._post(
            route=f"/query/{query.query_id}/execute",
            params=params,
        )
        try:
            return ExecutionResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ExecutionResponse", err) from err

    async def get_status(self, job_id: str) -> ExecutionStatusResponse:
        """GET status from Dune API for `job_id` (aka `execution_id`)"""
        response_json = await self._get(route=f"/execution/{job_id}/status")
        try:
            return ExecutionStatusResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ExecutionStatusResponse", err) from err

    async def get_result(self, job_id: str) -> ResultsResponse:
        """GET results from Dune API for `job_id` (aka `execution_id`)"""
        response_json = await self._get(route=f"/execution/{job_id}/results")
        try:
            return ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    async def get_result_csv(self, job_id: str) -> ExecutionResultCSV:
        """
        GET results in CSV format from Dune API for `job_id` (aka `execution_id`)

        this API only returns the raw data in CSV format, it is faster & lighterweight
        use this method for large results where you want lower CPU and memory overhead
        if you need metadata information use get_results() or get_status()
        """
        route = f"/execution/{job_id}/results/csv"
        url = self._route_url(f"/execution/{job_id}/results/csv")
        self.logger.debug(f"GET CSV received input url={url}")
        response = await self._get(route=route, raw=True)
        response.raise_for_status()
        return ExecutionResultCSV(data=BytesIO(await response.content.read(-1)))

    async def get_latest_result(
        self, query: Union[QueryBase, str, int]
    ) -> ResultsResponse:
        """
        GET the latest results for a query_id without having to execute the query again.

        :param query: :class:`Query` object OR query id as string | int

        https://dune.com/docs/api/api-reference/latest_results/
        """
        params, query_id = parse_query_object_or_id(query)
        response_json = await self._get(
            route=f"/query/{query_id}/results",
            params=params,
        )
        try:
            return ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    async def cancel_execution(self, job_id: str) -> bool:
        """POST Execution Cancellation to Dune API for `job_id` (aka `execution_id`)"""
        response_json = await self._post(
            route=f"/execution/{job_id}/cancel",
            params=None,
        )
        try:
            # No need to make a dataclass for this since it's just a boolean.
            success: bool = response_json["success"]
            return success
        except KeyError as err:
            raise DuneError(response_json, "CancellationResponse", err) from err

    async def _refresh(
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
        job_id = (await self.execute(query=query, performance=performance)).execution_id
        status = await self.get_status(job_id)
        while status.state not in ExecutionState.terminal_states():
            self.logger.info(
                f"waiting for query execution {job_id} to complete: {status}"
            )
            await asyncio.sleep(ping_frequency)
            status = await self.get_status(job_id)
        if status.state == ExecutionState.FAILED:
            self.logger.error(status)
            raise QueryFailed(f"Error data: {status.error}")

        return job_id

    async def refresh(
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
        job_id = await self._refresh(
            query, ping_frequency=ping_frequency, performance=performance
        )
        return await self.get_result(job_id)

    async def refresh_csv(
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
        job_id = await self._refresh(
            query, ping_frequency=ping_frequency, performance=performance
        )
        return await self.get_result_csv(job_id)

    async def refresh_into_dataframe(
        self, query: QueryBase, performance: Optional[str] = None
    ) -> Any:
        """
        Execute a Dune Query, waits till execution completes,
        fetched and returns the result as a Pandas DataFrame

        This is a convenience method that uses refresh_csv underneath
        """
        try:
            import pandas  # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise ImportError(
                "dependency failure, pandas is required but missing"
            ) from exc
        data = (await self.refresh_csv(query, performance=performance)).data
        return pandas.read_csv(data)
