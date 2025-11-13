""" "
Async Dune Client Class responsible for refreshing Dune Queries
Framework built on Dune's API Documentation
https://docs.dune.com/api-reference/overview/introduction
"""

from __future__ import annotations

import asyncio
import ssl
from io import BytesIO
from typing import TYPE_CHECKING, Any, Self, TypeVar

import certifi

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

from aiohttp import (
    ClientError,
    ClientResponse,
    ClientSession,
    ClientTimeout,
    ContentTypeError,
    TCPConnector,
)
from deprecated import deprecated

from dune_client.api.base import (
    DUNE_CSV_NEXT_OFFSET_HEADER,
    DUNE_CSV_NEXT_URI_HEADER,
    MAX_NUM_ROWS_PER_BATCH,
    BaseDuneClient,
)
from dune_client.models import (
    DuneError,
    ExecutionResponse,
    ExecutionResultCSV,
    ExecutionState,
    ExecutionStatusResponse,
    PipelineExecutionResponse,
    PipelineStatusResponse,
    QueryFailedError,
    ResultsResponse,
)
from dune_client.query import QueryBase, parse_query_object_or_id

PaginatedResult = TypeVar("PaginatedResult", ResultsResponse, ExecutionResultCSV)


class AsyncDuneClient(BaseDuneClient):
    """
    An asynchronous interface for Dune API with a few convenience methods
    combining the use of endpoints (e.g. refresh)

    Must be used as an async context manager:
        async with AsyncDuneClient() as client:
            results = await client.refresh(query)
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        request_timeout: float | None = None,
        client_version: str = "v1",
        performance: str = "medium",
        connection_limit: int = 3,
    ):
        """
        api_key - Dune API key
        connection_limit - number of parallel requests to execute.
        For non-pro accounts Dune allows only up to 3 requests but that number can be increased.
        """
        super().__init__(api_key, base_url, request_timeout, client_version, performance)
        self._connection_limit = connection_limit
        self._session: ClientSession | None = None
        self._max_attempts = 5
        self._retry_backoff = 0.5
        self._retry_statuses = {429, 502, 503, 504}

    async def __aenter__(self) -> Self:
        if self._session is not None:
            raise RuntimeError("AsyncDuneClient session already active")
        await self.connect()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
    ) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        if self._session is not None:
            raise RuntimeError("AsyncDuneClient session already active")
        self._session = self._create_session()

    async def disconnect(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _create_session(self) -> ClientSession:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = TCPConnector(limit=self._connection_limit, ssl=ssl_context)
        return ClientSession(
            connector=connector,
            base_url=self.base_url,
            timeout=ClientTimeout(total=self.request_timeout),
        )

    async def _handle_response(self, response: ClientResponse) -> Any:
        try:
            # Some responses can be decoded and converted to DuneErrors
            response_json = await response.json()
            self.logger.debug(f"received response {response_json}")
        except ContentTypeError as err:
            # Others can't. Only raise HTTP error for not decodable errors
            response.raise_for_status()
            raise ValueError("Unreachable since previous line raises") from err
        else:
            response.raise_for_status()
            return response_json

    def _route_url(self, route: str | None = None, url: str | None = None) -> str:
        if route is not None:
            return f"{self.api_version}{route}"
        if url is None:
            raise ValueError("Either route or url must be provided")
        assert url.startswith(self.base_url)
        return url[len(self.base_url) :]

    async def _get(
        self,
        route: str | None = None,
        params: Any | None = None,
        raw: bool = False,
        url: str | None = None,
    ) -> Any:
        return await self._request(
            method="GET",
            route=route,
            url=url,
            params=params,
            raw=raw,
        )

    async def _post(self, route: str, params: Any) -> Any:
        return await self._request(
            method="POST",
            route=route,
            json_body=params,
        )

    async def _request(
        self,
        *,
        method: str,
        route: str | None = None,
        url: str | None = None,
        params: Any | None = None,
        json_body: Any | None = None,
        raw: bool = False,
    ) -> Any:
        session = self._require_session()
        target = self._route_url(route=route, url=url) if route or url else None
        if target is None:
            raise ValueError("Either route or url must be provided")
        self.logger.debug(f"{method} received input target={target}")

        attempt = 0
        delay = self._retry_backoff
        while True:
            try:
                response = await session.request(
                    method,
                    target,
                    headers=self.default_headers(),
                    params=params,
                    json=json_body,
                )
            except ClientError:
                if attempt >= self._max_attempts - 1:
                    raise
                await asyncio.sleep(delay)
                attempt += 1
                delay *= 2
                continue

            if response.status in self._retry_statuses and attempt < self._max_attempts - 1:
                await response.read()
                response.release()
                await asyncio.sleep(delay)
                attempt += 1
                delay *= 2
                continue

            if raw:
                return response
            return await self._handle_response(response)

    async def execute_query(
        self, query: QueryBase, performance: str | None = None
    ) -> ExecutionResponse:
        """Post's to Dune API for execute `query`"""
        params = query.request_format()
        params["performance"] = performance or self.performance

        self.logger.info(f"executing {query.query_id} on {performance or self.performance} cluster")
        response_json = await self._post(
            route=f"/query/{query.query_id}/execute",
            params=params,
        )
        try:
            return ExecutionResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ExecutionResponse", err) from err

    async def execute_query_pipeline(
        self, query_id: int, performance: str | None = None
    ) -> PipelineExecutionResponse:
        """Post's to Dune API for execute query pipeline"""
        params: dict[str, str] = {}
        if performance is not None:
            params["performance"] = performance

        self.logger.info(f"executing pipeline for query {query_id}")
        response_json = await self._post(
            route=f"/query/{query_id}/pipeline/execute",
            params=params,
        )
        try:
            return PipelineExecutionResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "PipelineExecutionResponse", err) from err

    async def get_pipeline_status(self, pipeline_execution_id: str) -> PipelineStatusResponse:
        """GET pipeline execution status"""
        response_json = await self._get(
            route=f"/pipelines/executions/{pipeline_execution_id}/status"
        )
        try:
            return PipelineStatusResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "PipelineStatusResponse", err) from err

    async def get_execution_status(self, job_id: str) -> ExecutionStatusResponse:
        """GET status from Dune API for `job_id` (aka `execution_id`)"""
        response_json = await self._get(route=f"/execution/{job_id}/status")
        try:
            return ExecutionStatusResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ExecutionStatusResponse", err) from err

    async def get_execution_results(
        self,
        job_id: str,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ResultsResponse:
        """GET results from Dune API for `job_id` (aka `execution_id`)"""
        self._validate_sampling(sample_count, batch_size, filters)

        if sample_count is None and batch_size is None:
            batch_size = MAX_NUM_ROWS_PER_BATCH

        return await self._collect_pages(
            lambda: self._get_result_page(
                job_id,
                columns=columns,
                sample_count=sample_count,
                filters=filters,
                sort_by=sort_by,
                limit=batch_size,
            ),
            self._get_result_by_url,
        )

    async def get_execution_results_csv(
        self,
        job_id: str,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ExecutionResultCSV:
        """
        GET results in CSV format from Dune API for `job_id` (aka `execution_id`)

        this API only returns the raw data in CSV format, it is faster & lighterweight
        use this method for large results where you want lower CPU and memory overhead
        if you need metadata information use get_execution_results() or get_execution_status()
        """
        self._validate_sampling(sample_count, batch_size, filters)

        if sample_count is None and batch_size is None:
            batch_size = MAX_NUM_ROWS_PER_BATCH

        return await self._collect_pages(
            lambda: self._get_result_csv_page(
                job_id,
                columns=columns,
                sample_count=sample_count,
                filters=filters,
                sort_by=sort_by,
                limit=batch_size,
            ),
            self._get_result_csv_by_url,
        )

    async def get_latest_result(
        self,
        query: QueryBase | str | int,
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

        async def first_page() -> ResultsResponse:
            response_json = await self._get(
                route=f"/query/{query_id}/results",
                params=params,
            )
            try:
                return ResultsResponse.from_dict(response_json)
            except KeyError as err:
                raise DuneError(response_json, "ResultsResponse", err) from err

        return await self._collect_pages(first_page, self._get_result_by_url)

    async def cancel_execution(self, job_id: str) -> bool:
        response_json = await self._post(
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

    #######################
    # Deprecated Functions:
    #######################
    @deprecated(version="1.9.3", reason="Please use execute_query")
    async def execute(self, query: QueryBase, performance: str | None = None) -> ExecutionResponse:
        """Post's to Dune API for execute `query`"""
        return await self.execute_query(query, performance)

    @deprecated(version="1.9.3", reason="Please use get_execution_status")
    async def get_status(self, job_id: str) -> ExecutionStatusResponse:
        """GET status from Dune API for `job_id` (aka `execution_id`)"""
        return await self.get_execution_status(job_id)

    @deprecated(version="1.9.3", reason="Please use get_execution_results")
    async def get_result(
        self,
        job_id: str,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ResultsResponse:
        """GET results from Dune API for `job_id` (aka `execution_id`)"""
        return await self.get_execution_results(
            job_id,
            batch_size=batch_size,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
        )

    @deprecated(version="1.9.3", reason="Please use get_execution_results_csv")
    async def get_result_csv(
        self,
        job_id: str,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ExecutionResultCSV:
        """
        GET results in CSV format from Dune API for `job_id` (aka `execution_id`)

        this API only returns the raw data in CSV format, it is faster & lighterweight
        use this method for large results where you want lower CPU and memory overhead
        if you need metadata information use get_execution_results() or get_execution_status()
        """
        return await self.get_execution_results_csv(
            job_id,
            batch_size=batch_size,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
        )

    ########################
    # Higher level functions
    ########################

    async def run_query(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
        performance: str | None = None,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ResultsResponse:
        """
        Executes a Dune `query`, waits until execution completes,
        fetches and returns the results.
        Sleeps `ping_frequency` seconds between each status request.
        """
        self._validate_sampling(sample_count, batch_size, filters)

        job_id = await self._refresh(query, ping_frequency=ping_frequency, performance=performance)
        return await self.get_execution_results(
            job_id,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            batch_size=batch_size,
        )

    async def run_query_csv(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
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
        self._validate_sampling(sample_count, batch_size, filters)

        job_id = await self._refresh(query, ping_frequency=ping_frequency, performance=performance)
        return await self.get_execution_results_csv(
            job_id,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            batch_size=batch_size,
        )

    async def run_query_dataframe(
        self,
        query: QueryBase,
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
        results = await self.run_query_csv(
            query,
            performance=performance,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            batch_size=batch_size,
        )
        return pd.read_csv(results.data)

    ######################
    # Deprecated Functions
    ######################
    @deprecated(version="1.9.3", reason="Please use run_query")
    async def refresh(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
        performance: str | None = None,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ResultsResponse:
        """
        Executes a Dune `query`, waits until execution completes,
        fetches and returns the results.
        Sleeps `ping_frequency` seconds between each status request.
        """
        return await self.run_query(
            query,
            ping_frequency=ping_frequency,
            performance=performance,
            batch_size=batch_size,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
        )

    @deprecated(version="1.9.3", reason="Please use run_query_csv")
    async def refresh_csv(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
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
        return await self.run_query_csv(
            query,
            ping_frequency=ping_frequency,
            performance=performance,
            batch_size=batch_size,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
        )

    @deprecated(version="1.9.3", reason="Please use run_query_dataframe")
    async def refresh_into_dataframe(
        self,
        query: QueryBase,
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

        This is a convenience method that uses run_query_csv underneath
        """
        return await self.run_query_dataframe(
            query,
            performance=performance,
            batch_size=batch_size,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
        )

    #################
    # Private Methods
    #################

    async def _get_result_page(
        self,
        job_id: str,
        limit: int | None = None,
        offset: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ResultsResponse:
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
        params: dict[str, Any] | None = None,
    ) -> ResultsResponse:
        response_json = await self._get(url=url, params=params)

        try:
            return ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err

    async def _get_result_csv_page(
        self,
        job_id: str,
        limit: int | None = None,
        offset: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ExecutionResultCSV:
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
        next_offset_header = response.headers.get(DUNE_CSV_NEXT_OFFSET_HEADER)
        next_offset = self._parse_next_offset(next_offset_header)
        data = BytesIO(await response.content.read(-1))
        response.release()
        return ExecutionResultCSV(
            data=data,
            next_uri=next_uri,
            next_offset=next_offset,
        )

    async def _get_result_csv_by_url(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> ExecutionResultCSV:
        response = await self._get(url=url, params=params, raw=True)
        response.raise_for_status()

        next_uri = response.headers.get(DUNE_CSV_NEXT_URI_HEADER)
        next_offset_header = response.headers.get(DUNE_CSV_NEXT_OFFSET_HEADER)
        next_offset = self._parse_next_offset(next_offset_header)
        data = BytesIO(await response.content.read(-1))
        response.release()
        return ExecutionResultCSV(
            data=data,
            next_uri=next_uri,
            next_offset=next_offset,
        )

    async def _refresh(
        self,
        query: QueryBase,
        ping_frequency: int = 5,
        performance: str | None = None,
    ) -> str:
        job_id = (await self.execute_query(query=query, performance=performance)).execution_id
        terminal_states = ExecutionState.terminal_states()

        while True:
            status = await self.get_execution_status(job_id)
            if status.state in terminal_states:
                if status.state == ExecutionState.FAILED:
                    self.logger.error(status)
                    if status.error:
                        raise QueryFailedError(status.error.message)
                    raise QueryFailedError("Query execution failed")
                return job_id

            self.logger.info(f"waiting for query execution {job_id} to complete: {status}")
            await asyncio.sleep(ping_frequency)

    def _require_session(self) -> ClientSession:
        if self._session is None:
            raise RuntimeError("AsyncDuneClient must be used as an async context manager")
        return self._session

    @staticmethod
    def _validate_sampling(
        sample_count: int | None,
        batch_size: int | None,
        filters: str | None,
    ) -> None:
        assert sample_count is None or (batch_size is None and filters is None), (
            "sampling cannot be combined with filters or pagination"
        )

    def _parse_next_offset(self, header_value: str | None) -> int | None:
        if header_value is None:
            return None
        try:
            return int(header_value)
        except ValueError:
            self.logger.warning(
                "invalid x-dune-next-offset header encountered; ignoring",
                extra={"header_value": header_value},
            )
            return None

    async def _collect_pages(
        self,
        fetch_first: Callable[[], Awaitable[PaginatedResult]],
        fetch_next: Callable[[str], Awaitable[PaginatedResult]],
    ) -> PaginatedResult:
        results = await fetch_first()
        while results.next_uri is not None:
            results += await fetch_next(results.next_uri)
        return results
