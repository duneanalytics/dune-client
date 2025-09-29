"""
Simplified Async Dune Client - A thin async wrapper around the sync client logic
"""

from __future__ import annotations

import asyncio
from io import BytesIO
from typing import Any, Self

from aiohttp import ClientSession, ClientTimeout

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
    QueryFailedError,
    ResultsResponse,
)
from dune_client.query import QueryBase, parse_query_object_or_id


class AsyncDuneClient(BaseDuneClient):
    """
    A simplified asynchronous interface for Dune API.
    Reuses the sync client's logic for parameter validation and building.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        request_timeout: float | None = None,
        client_version: str = "v1",
        performance: str = "medium",
    ):
        super().__init__(api_key, base_url, request_timeout, client_version, performance)
        self._session: ClientSession | None = None

    async def __aenter__(self) -> Self:
        # Simple session creation
        timeout = ClientTimeout(total=self.request_timeout)
        self._session = ClientSession(
            base_url=self.base_url,
            timeout=timeout,
        )
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
    ) -> None:
        if self._session:
            await self._session.close()

    async def _request(
        self,
        method: str,
        route: str | None = None,
        url: str | None = None,
        json: Any | None = None,
        params: Any | None = None,
        raw: bool = False,
    ) -> Any:
        """Unified request method that handles all HTTP methods"""
        if self._session is None:
            raise ValueError("Client is not connected; use 'async with AsyncDuneClient()'")

        # Handle both route and full URL
        if url is not None:
            # If it's a full URL, extract the path part for use with base_url session
            final_url = url.removeprefix(self.base_url)
        elif route is not None:
            final_url = f"{self.api_version}{route}"
        else:
            raise ValueError("Either route or url must be provided")

        async with self._session.request(
            method=method,
            url=final_url,
            json=json,
            params=params,
            headers=self.default_headers(),
        ) as response:
            if raw:
                return response

            response.raise_for_status()
            response_json = await response.json()
            self.logger.debug(f"received response {response_json}")
            return response_json

    async def execute(self, query: QueryBase, performance: str | None = None) -> ExecutionResponse:
        """Execute a query"""
        params = query.request_format()
        params["performance"] = performance or self.performance

        self.logger.info(f"executing {query.query_id} on {performance or self.performance} cluster")
        response_json = await self._request(
            "POST",
            route=f"/query/{query.query_id}/execute",
            json=params,
        )
        try:
            return ExecutionResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ExecutionResponse", err) from err

    async def get_status(self, job_id: str) -> ExecutionStatusResponse:
        """Get execution status"""
        response_json = await self._request("GET", route=f"/execution/{job_id}/status")
        try:
            return ExecutionStatusResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ExecutionStatusResponse", err) from err

    async def get_result(
        self,
        job_id: str,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ResultsResponse:
        """Get execution results with pagination"""
        # Reuse parameter validation from base class
        params = self._build_parameters(
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=batch_size or MAX_NUM_ROWS_PER_BATCH,
            offset=0,
        )

        response_json = await self._request(
            "GET",
            route=f"/execution/{job_id}/results",
            params=params,
        )

        try:
            results = ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err
        else:
            # Handle pagination
            while results.next_uri:
                response_json = await self._request("GET", url=results.next_uri)
                batch = ResultsResponse.from_dict(response_json)
                results += batch

            return results

    async def get_result_csv(
        self,
        job_id: str,
        batch_size: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ExecutionResultCSV:
        """Get execution results in CSV format with pagination"""
        params = self._build_parameters(
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=batch_size or MAX_NUM_ROWS_PER_BATCH,
            offset=0,
        )

        # First request
        async with self._session.get(
            f"{self.api_version}/execution/{job_id}/results/csv",
            params=params,
            headers=self.default_headers(),
        ) as response:
            response.raise_for_status()
            data = BytesIO(await response.read())
            next_uri = response.headers.get(DUNE_CSV_NEXT_URI_HEADER)
            next_offset = response.headers.get(DUNE_CSV_NEXT_OFFSET_HEADER)

        result = ExecutionResultCSV(data=data, next_uri=next_uri, next_offset=next_offset)

        # Handle pagination
        while result.next_uri:
            # Extract path from full URL for use with base_url session
            next_path = result.next_uri.removeprefix(self.base_url)
            async with self._session.get(
                next_path,
                headers=self.default_headers(),
            ) as response:
                response.raise_for_status()
                batch_data = BytesIO(await response.read())
                next_uri = response.headers.get(DUNE_CSV_NEXT_URI_HEADER)
                next_offset = response.headers.get(DUNE_CSV_NEXT_OFFSET_HEADER)

            batch = ExecutionResultCSV(data=batch_data, next_uri=next_uri, next_offset=next_offset)
            result += batch

        return result

    async def get_latest_result(
        self,
        query: QueryBase | str | int,
        batch_size: int = MAX_NUM_ROWS_PER_BATCH,
    ) -> ResultsResponse:
        """Get the latest results for a query without re-executing"""
        params, query_id = parse_query_object_or_id(query)
        if params is None:
            params = {}
        params["limit"] = batch_size

        response_json = await self._request(
            "GET",
            route=f"/query/{query_id}/results",
            params=params,
        )

        try:
            results = ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err
        else:
            # Handle pagination
            while results.next_uri:
                response_json = await self._request("GET", url=results.next_uri)
                batch = ResultsResponse.from_dict(response_json)
                results += batch

            return results

    async def cancel_execution(self, job_id: str) -> bool:
        """Cancel an execution"""
        response_json = await self._request("POST", route=f"/execution/{job_id}/cancel")
        try:
            return response_json["success"]
        except KeyError as err:
            raise DuneError(response_json, "CancellationResponse", err) from err

    # High-level convenience methods

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
        """Execute a query and wait for results"""
        job_id = (await self.execute(query, performance)).execution_id

        # Wait for completion
        status = await self.get_status(job_id)
        while status.state not in ExecutionState.terminal_states():
            self.logger.info(f"waiting for query execution {job_id} to complete: {status}")
            await asyncio.sleep(ping_frequency)
            status = await self.get_status(job_id)

        if status.state == ExecutionState.FAILED:
            self.logger.error(status)
            raise QueryFailedError(f"Error data: {status.error}")

        return await self.get_result(
            job_id,
            batch_size=batch_size,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
        )

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
        """Execute a query and get results in CSV format"""
        job_id = (await self.execute(query, performance)).execution_id

        # Wait for completion
        status = await self.get_status(job_id)
        while status.state not in ExecutionState.terminal_states():
            self.logger.info(f"waiting for query execution {job_id} to complete: {status}")
            await asyncio.sleep(ping_frequency)
            status = await self.get_status(job_id)

        if status.state == ExecutionState.FAILED:
            self.logger.error(status)
            raise QueryFailedError(f"Error data: {status.error}")

        return await self.get_result_csv(
            job_id,
            batch_size=batch_size,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
        )

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
        """Execute a query and return results as a pandas DataFrame"""
        try:
            import pandas as pd  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError("dependency failure, pandas is required but missing") from exc

        results = await self.refresh_csv(
            query,
            performance=performance,
            batch_size=batch_size,
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
        )
        return pd.read_csv(results.data)

    # For backwards compatibility
    async def connect(self) -> None:
        """Opens a client session (can be used instead of async with)"""
        if self._session is None:
            timeout = ClientTimeout(total=self.request_timeout)
            self._session = ClientSession(
                base_url=self.base_url,
                timeout=timeout,
            )

    async def disconnect(self) -> None:
        """Closes client session"""
        if self._session:
            await self._session.close()
