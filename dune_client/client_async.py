""""
Async Dune Client Class responsible for refreshing Dune Queries
Framework built on Dune's API Documentation
https://docs.dune.com/api-reference/overview/introduction
"""

from __future__ import annotations

import asyncio
import ssl
from io import BytesIO
from typing import Any, Callable, Dict, List, Optional, Union

import certifi
from aiohttp import (
    ClientResponseError,
    ClientSession,
    ClientResponse,
    ContentTypeError,
    TCPConnector,
    ClientTimeout,
)

from dune_client.api.base import (
    BaseDuneClient,
    DUNE_CSV_NEXT_URI_HEADER,
    DUNE_CSV_NEXT_OFFSET_HEADER,
    MAX_NUM_ROWS_PER_BATCH,
)
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


class RetryableError(Exception):
    """
    Internal exception used to signal that the request should be retried
    """

    def __init__(self, base_error: ClientResponseError) -> None:
        self.base_error = base_error


class MaxRetryError(Exception):
    """
    This exception is raised when the maximum number of retries is exceeded,
    e.g. due to rate limiting or internal server errors
    """

    def __init__(self, url: str, reason: Exception | None = None) -> None:
        self.reason = reason

        message = f"Max retries exceeded with url: {url} (Caused by {reason!r})"

        super().__init__(message)


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
        # Create an SSL context using the certifi certificate store
        ssl_context = ssl.create_default_context(cafile=certifi.where())

        conn = TCPConnector(limit=self._connection_limit, ssl=ssl_context)
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
        if response.status in {429, 502, 503, 504}:
            try:
                response.raise_for_status()
            except ClientResponseError as err:
                raise RetryableError(
                    base_error=err,
                ) from err
        try:
            # Some responses can be decoded and converted to DuneErrors
            response_json = await response.json()
            self.logger.debug(f"received response {response_json}")
            return response_json
        except ContentTypeError as err:
            # Others can't. Only raise HTTP error for not decodable errors
            response.raise_for_status()
            raise ValueError("Unreachable since previous line raises") from err

    def _route_url(
        self,
        route: Optional[str] = None,
        url: Optional[str] = None,
    ) -> str:
        if route is not None:
            final_route = f"{self.api_version}{route}"
        elif url is not None:
            assert url.startswith(self.base_url)
            final_route = url[len(self.base_url) :]
        else:
            assert route is not None or url is not None

        return final_route

    async def _handle_ratelimit(self, call: Callable[..., Any], url: str) -> Any:
        """Generic wrapper around request callables. If the request fails due to rate limiting,
        or server side errors, it will retry it up to five times, sleeping i * 5s in between
        """
        backoff_factor = 0.5
        error: Optional[ClientResponseError] = None
        for i in range(5):
            try:
                return await call()
            except RetryableError as e:
                self.logger.warning(
                    f"Rate limited or internal error. Retrying in {i * 5} seconds..."
                )
                error = e.base_error
                await asyncio.sleep(i**2 * backoff_factor)

        raise MaxRetryError(url, error)

    async def _get(
        self,
        route: Optional[str] = None,
        params: Optional[Any] = None,
        raw: bool = False,
        url: Optional[str] = None,
    ) -> Any:
        final_route = self._route_url(route=route, url=url)
        self.logger.debug(f"GET received input route={final_route}")

        async def _get() -> Any:
            if self._session is None:
                raise ValueError("Client is not connected; call `await cl.connect()`")
            response = await self._session.get(
                url=final_route,
                headers=self.default_headers(),
                params=params,
            )
            if raw:
                return response
            return await self._handle_response(response)

        return await self._handle_ratelimit(_get, final_route)

    async def _post(self, route: str, params: Any) -> Any:
        url = self._route_url(route)
        self.logger.debug(f"POST received input url={url}, params={params}")

        async def _post() -> Any:
            if self._session is None:
                raise ValueError("Client is not connected; call `await cl.connect()`")
            response = await self._session.post(
                url=url,
                json=params,
                headers=self.default_headers(),
            )
            return await self._handle_response(response)

        return await self._handle_ratelimit(_post, route)

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

    async def get_result(
        self,
        job_id: str,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ResultsResponse:
        """GET results from Dune API for `job_id` (aka `execution_id`)"""
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (batch_size is None and filters is None)
        ), "sampling cannot be combined with filters or pagination"

        if sample_count is None and batch_size is None:
            batch_size = MAX_NUM_ROWS_PER_BATCH

        results = await self._get_result_page(
            job_id,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=batch_size,
        )
        while results.next_uri is not None:
            batch = await self._get_result_by_url(results.next_uri)
            results += batch

        return results

    async def get_result_csv(
        self,
        job_id: str,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ExecutionResultCSV:
        """
        GET results in CSV format from Dune API for `job_id` (aka `execution_id`)

        this API only returns the raw data in CSV format, it is faster & lighterweight
        use this method for large results where you want lower CPU and memory overhead
        if you need metadata information use get_results() or get_status()
        """
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (batch_size is None and filters is None)
        ), "sampling cannot be combined with filters or pagination"

        if sample_count is None and batch_size is None:
            batch_size = MAX_NUM_ROWS_PER_BATCH

        results = await self._get_result_csv_page(
            job_id,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=batch_size,
        )
        while results.next_uri is not None:
            batch = await self._get_result_csv_by_url(results.next_uri)
            results += batch

        return results

    async def get_latest_result(
        self,
        query: Union[QueryBase, str, int],
        batch_size: int = MAX_NUM_ROWS_PER_BATCH,
    ) -> ResultsResponse:
        """
        GET the latest results for a query_id without having to execute the query again.

        :param query: :class:`Query` object OR query id as string | int

        https://docs.dune.com/api-reference/executions/endpoint/get-query-result
        """
        params, query_id = parse_query_object_or_id(query)

        if params is None:
            params = {}

        params["limit"] = batch_size

        response_json = await self._get(
            route=f"/query/{query_id}/results",
            params=params,
        )
        try:
            results = ResultsResponse.from_dict(response_json)
            while results.next_uri is not None:
                batch = await self._get_result_by_url(results.next_uri)
                results += batch

            return results
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

    ########################
    # Higher level functions
    ########################

    async def refresh(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
        performance: Optional[str] = None,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ResultsResponse:
        """
        Executes a Dune `query`, waits until execution completes,
        fetches and returns the results.
        Sleeps `ping_frequency` seconds between each status request.
        """
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (batch_size is None and filters is None)
        ), "sampling cannot be combined with filters or pagination"

        job_id = await self._refresh(
            query, ping_frequency=ping_frequency, performance=performance
        )
        return await self.get_result(
            job_id,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            batch_size=batch_size,
        )

    async def refresh_csv(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
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
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (batch_size is None and filters is None)
        ), "sampling cannot be combined with filters or pagination"

        job_id = await self._refresh(
            query, ping_frequency=ping_frequency, performance=performance
        )
        return await self.get_result_csv(
            job_id,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            batch_size=batch_size,
        )

    async def refresh_into_dataframe(
        self,
        query: QueryBase,
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

        This is a convenience method that uses refresh_csv underneath
        """
        try:
            import pandas  # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise ImportError(
                "dependency failure, pandas is required but missing"
            ) from exc
        results = await self.refresh_csv(
            query,
            performance=performance,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            batch_size=batch_size,
        )
        return pandas.read_csv(results.data)

    #################
    # Private Methods
    #################

    async def _get_result_page(
        self,
        job_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ResultsResponse:
        """GET a page of results from Dune API for `job_id` (aka `execution_id`)"""

        if sample_count is None and limit is None and offset is None:
            limit = MAX_NUM_ROWS_PER_BATCH
            offset = 0

        params = self._build_parameters(
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
        )
        response_json = await self._get(
            route=f"/execution/{job_id}/results",
            params=params,
        )

        try:
            return ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    async def _get_result_by_url(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> ResultsResponse:
        """
        GET results from Dune API with a given URL. This is particularly useful for pagination.
        """
        response_json = await self._get(url=url, params=params)

        try:
            return ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    async def _get_result_csv_page(
        self,
        job_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ExecutionResultCSV:
        """
        GET a page of results in CSV format from Dune API for `job_id` (aka `execution_id`)
        """

        if sample_count is None and limit is None and offset is None:
            limit = MAX_NUM_ROWS_PER_BATCH
            offset = 0

        params = self._build_parameters(
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
        )

        route = f"/execution/{job_id}/results/csv"
        response = await self._get(route=route, params=params, raw=True)
        response.raise_for_status()

        next_uri = response.headers.get(DUNE_CSV_NEXT_URI_HEADER)
        next_offset = response.headers.get(DUNE_CSV_NEXT_OFFSET_HEADER)
        return ExecutionResultCSV(
            data=BytesIO(await response.content.read(-1)),
            next_uri=next_uri,
            next_offset=next_offset,
        )

    async def _get_result_csv_by_url(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> ExecutionResultCSV:
        """
        GET results in CSV format from Dune API with a given URL.
        This is particularly useful for pagination.
        """
        response = await self._get(url=url, params=params, raw=True)
        response.raise_for_status()

        next_uri = response.headers.get(DUNE_CSV_NEXT_URI_HEADER)
        next_offset = response.headers.get(DUNE_CSV_NEXT_OFFSET_HEADER)
        return ExecutionResultCSV(
            data=BytesIO(await response.content.read(-1)),
            next_uri=next_uri,
            next_offset=next_offset,
        )

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
