"""Unit tests for AsyncDuneClient core logic"""

from datetime import datetime
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock

import aiounittest
import pytest
from aiohttp import ClientResponse

from dune_client.client_async import AsyncDuneClient
from dune_client.models import (
    ExecutionResult,
    ExecutionState,
    ExecutionStatusResponse,
    QueryFailedError,
    ResultMetadata,
    ResultsResponse,
    TimeData,
)
from dune_client.query import QueryBase


class TestPaginationLogic(aiounittest.AsyncTestCase):
    """Test _collect_pages pagination behavior"""

    def _create_client(self):
        client = AsyncDuneClient(api_key="test")
        client._session = MagicMock()
        client._session.request = AsyncMock()
        client._session.close = AsyncMock()
        return client

    def _create_results_response(
        self,
        rows,
        *,
        execution_id="job-123",
        next_uri=None,
        next_offset=None,
    ) -> ResultsResponse:
        """Create a real ResultsResponse instance for pagination testing"""

        column_names = list(rows[0].keys()) if rows else ["id"]
        column_types = ["text"] * len(column_names)
        metadata = ResultMetadata(
            column_names=column_names,
            column_types=column_types,
            row_count=len(rows),
            result_set_bytes=len(rows),
            total_row_count=len(rows),
            total_result_set_bytes=len(rows),
            datapoint_count=len(rows) * len(column_names),
            pending_time_millis=None,
            execution_time_millis=0,
        )

        result = ExecutionResult(rows=list(rows), metadata=metadata)

        return ResultsResponse(
            execution_id=execution_id,
            query_id=12345,
            state=ExecutionState.COMPLETED,
            times=TimeData(
                submitted_at=datetime.utcnow(),
                execution_started_at=None,
                execution_ended_at=None,
                expires_at=None,
                cancelled_at=None,
            ),
            result=result,
            next_uri=next_uri,
            next_offset=next_offset,
        )

    async def test_single_page_no_pagination(self):
        """Test that single page results don't trigger pagination"""
        client = self._create_client()

        first_page = self._create_results_response([{"id": 1}, {"id": 2}], next_uri=None)

        async def fetch_first():
            return first_page

        async def fetch_next(_url):
            raise AssertionError("Should not fetch next page")

        result = await client._collect_pages(fetch_first, fetch_next)
        assert result.result.rows == [{"id": 1}, {"id": 2}]
        assert result.next_uri is None

    async def test_multiple_pages_collected(self):
        """Test that multiple pages are correctly combined"""
        client = self._create_client()

        page1 = self._create_results_response(
            [{"id": 1}],
            next_uri="http://page2",
            next_offset=1,
        )
        page2 = self._create_results_response(
            [{"id": 2}],
            next_uri="http://page3",
            next_offset=2,
        )
        page3 = self._create_results_response(
            [{"id": 3}],
            next_uri=None,
            next_offset=None,
        )

        pages = {"http://page2": page2, "http://page3": page3}

        async def fetch_first():
            return page1

        async def fetch_next(url):
            return pages[url]

        result = await client._collect_pages(fetch_first, fetch_next)
        assert len(result.result.rows) == 3
        assert result.result.rows == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert result.result.metadata.row_count == 3
        assert result.next_uri is None
        assert result.next_offset is None

    async def test_empty_results(self):
        """Test pagination with empty result set"""
        client = self._create_client()

        empty_page = self._create_results_response([], next_uri=None)

        async def fetch_first():
            return empty_page

        async def fetch_next(_url):
            raise AssertionError("Should not fetch next page")

        result = await client._collect_pages(fetch_first, fetch_next)
        assert result.result.rows == []


class TestValidation(aiounittest.AsyncTestCase):
    """Test _validate_sampling parameter validation"""

    def test_sampling_with_filters_raises_assertion(self):
        """Test that sampling + filters raises clear error"""
        with pytest.raises(AssertionError) as exc_info:
            AsyncDuneClient._validate_sampling(
                sample_count=100, batch_size=None, filters="value > 10"
            )
        assert "sampling cannot be combined with filters or pagination" in str(exc_info.value)

    def test_sampling_with_batch_size_raises_assertion(self):
        """Test that sampling + pagination raises clear error"""
        with pytest.raises(AssertionError) as exc_info:
            AsyncDuneClient._validate_sampling(sample_count=100, batch_size=1000, filters=None)
        assert "sampling cannot be combined with filters or pagination" in str(exc_info.value)

    def test_sampling_alone_is_valid(self):
        """Test that sampling without filters/pagination is allowed"""
        # Should not raise
        AsyncDuneClient._validate_sampling(sample_count=100, batch_size=None, filters=None)

    def test_filters_and_pagination_without_sampling_is_valid(self):
        """Test that filters + pagination is allowed without sampling"""
        # Should not raise
        AsyncDuneClient._validate_sampling(sample_count=None, batch_size=1000, filters="value > 10")


class TestTerminalStateHandling(aiounittest.AsyncTestCase):
    """Test _refresh terminal state handling"""

    def _create_client(self):
        client = AsyncDuneClient(api_key="test")
        client._session = MagicMock()
        client._session.request = AsyncMock()
        client._session.close = AsyncMock()
        return client

    def _mock_status_response(self, state, error=None):
        """Create a mock ExecutionStatusResponse"""
        return ExecutionStatusResponse(
            execution_id="test-job-123",
            query_id=12345,
            state=state,
            times=TimeData(
                submitted_at=MagicMock(),
                execution_started_at=None,
                execution_ended_at=None,
                expires_at=None,
                cancelled_at=None,
            ),
            queue_position=None,
            result_metadata=None,
            error=error,
        )

    async def test_failed_state_raises_query_failed_error(self):
        """Test that FAILED state raises QueryFailedError"""
        client = self._create_client()
        query = QueryBase(name="test", query_id=123)

        # Mock execute_query to return a job_id
        client.execute_query = AsyncMock(return_value=MagicMock(execution_id="job-123"))

        # Mock get_execution_status to return FAILED
        failed_status = self._mock_status_response(
            ExecutionState.FAILED, error=MagicMock(message="Query syntax error")
        )
        client.get_execution_status = AsyncMock(return_value=failed_status)

        with pytest.raises(QueryFailedError) as exc_info:
            await client._refresh(query)

        assert "Query syntax error" in str(exc_info.value)

    async def test_completed_state_returns_job_id(self):
        """Test that COMPLETED state returns job_id successfully"""
        client = self._create_client()
        query = QueryBase(name="test", query_id=123)

        client.execute_query = AsyncMock(return_value=MagicMock(execution_id="job-123"))
        completed_status = self._mock_status_response(ExecutionState.COMPLETED)
        client.get_execution_status = AsyncMock(return_value=completed_status)

        job_id = await client._refresh(query)
        assert job_id == "job-123"

    async def test_cancelled_state_returns_job_id(self):
        """Test that CANCELLED state returns job_id (doesn't raise)"""
        client = self._create_client()
        query = QueryBase(name="test", query_id=123)

        client.execute_query = AsyncMock(return_value=MagicMock(execution_id="job-123"))
        cancelled_status = self._mock_status_response(ExecutionState.CANCELLED)
        client.get_execution_status = AsyncMock(return_value=cancelled_status)

        job_id = await client._refresh(query)
        assert job_id == "job-123"

    async def test_expired_state_returns_job_id(self):
        """Test that EXPIRED state returns job_id (doesn't raise)"""
        client = self._create_client()
        query = QueryBase(name="test", query_id=123)

        client.execute_query = AsyncMock(return_value=MagicMock(execution_id="job-123"))
        expired_status = self._mock_status_response(ExecutionState.EXPIRED)
        client.get_execution_status = AsyncMock(return_value=expired_status)

        job_id = await client._refresh(query)
        assert job_id == "job-123"

    async def test_partial_state_returns_job_id(self):
        """Test that PARTIAL state returns job_id (doesn't raise)"""
        client = self._create_client()
        query = QueryBase(name="test", query_id=123)

        client.execute_query = AsyncMock(return_value=MagicMock(execution_id="job-123"))
        partial_status = self._mock_status_response(ExecutionState.PARTIAL)
        client.get_execution_status = AsyncMock(return_value=partial_status)

        job_id = await client._refresh(query)
        assert job_id == "job-123"

    async def test_pending_to_completed_waits(self):
        """Test that PENDING/EXECUTING states wait, then COMPLETED succeeds"""
        client = self._create_client()
        query = QueryBase(name="test", query_id=123)

        client.execute_query = AsyncMock(return_value=MagicMock(execution_id="job-123"))

        # Sequence: PENDING -> EXECUTING -> COMPLETED
        statuses = [
            self._mock_status_response(ExecutionState.PENDING),
            self._mock_status_response(ExecutionState.EXECUTING),
            self._mock_status_response(ExecutionState.COMPLETED),
        ]
        client.get_execution_status = AsyncMock(side_effect=statuses)

        job_id = await client._refresh(query, ping_frequency=0)
        assert job_id == "job-123"
        assert client.get_execution_status.call_count == 3


class TestSessionRequirement(aiounittest.AsyncTestCase):
    """Test _require_session error handling"""

    def test_calling_without_context_manager_raises_helpful_error(self):
        """Test that using client without context manager gives clear error"""
        client = AsyncDuneClient(api_key="test")
        # Don't set up session

        with pytest.raises(RuntimeError) as exc_info:
            client._require_session()

        assert "async context manager" in str(exc_info.value)

    async def test_using_get_without_session_raises_error(self):
        """Test that calling _get without session setup raises"""
        client = AsyncDuneClient(api_key="test")
        # Don't connect or use context manager

        with pytest.raises(RuntimeError) as exc_info:
            await client._get(route="/test")

        assert "async context manager" in str(exc_info.value)


class TestCSVHeaderParsing(aiounittest.AsyncTestCase):
    """Test CSV result pagination header parsing"""

    def _create_client(self):
        client = AsyncDuneClient(api_key="test")
        client._session = MagicMock()
        client._session.request = AsyncMock()
        client._session.close = AsyncMock()
        return client

    def _mock_csv_response(self, csv_data=b"col1,col2\nval1,val2", next_uri=None, next_offset=None):
        """Create a mock CSV response"""
        response = MagicMock(spec=ClientResponse)
        response.status = 200
        response.headers = {}
        if next_uri:
            response.headers["x-dune-next-uri"] = next_uri
        if next_offset is not None:
            response.headers["x-dune-next-offset"] = str(next_offset)
        response.content = MagicMock()
        response.content.read = AsyncMock(return_value=csv_data)
        response.raise_for_status = MagicMock()
        response.release = MagicMock()
        return response

    async def test_csv_without_pagination_headers(self):
        """Test CSV response with no pagination headers"""
        client = self._create_client()

        csv_response = self._mock_csv_response(csv_data=b"id,name\n1,test")
        client._get = AsyncMock(return_value=csv_response)

        result = await client._get_result_csv_page("job-123")

        assert result.next_uri is None
        assert result.next_offset is None
        assert isinstance(result.data, BytesIO)
        csv_response.raise_for_status.assert_called_once()

    async def test_csv_with_pagination_headers(self):
        """Test CSV response with pagination headers present"""
        client = self._create_client()

        csv_response = self._mock_csv_response(
            csv_data=b"id,name\n1,test", next_uri="http://api.dune.com/next", next_offset=100
        )
        client._get = AsyncMock(return_value=csv_response)

        result = await client._get_result_csv_page("job-123")

        assert result.next_uri == "http://api.dune.com/next"
        assert result.next_offset == 100
        assert isinstance(result.data, BytesIO)
        csv_response.raise_for_status.assert_called_once()

    async def test_csv_response_is_released(self):
        """Test that CSV response resources are properly released"""
        client = self._create_client()

        csv_response = self._mock_csv_response()
        client._get = AsyncMock(return_value=csv_response)

        await client._get_result_csv_page("job-123")

        # Verify response.release() was called
        csv_response.release.assert_called_once()
        csv_response.raise_for_status.assert_called_once()

    async def test_csv_multi_page_combines_rows(self):
        """Test that CSV pagination merges subsequent pages without duplicate headers"""
        client = self._create_client()

        first_response = self._mock_csv_response(
            csv_data=b"id,name\n1,test\n",
            next_uri="http://api.dune.com/next",
            next_offset=100,
        )
        second_response = self._mock_csv_response(
            csv_data=b"id,name\n2,test2\n",
            next_uri=None,
            next_offset=200,
        )

        client._get = AsyncMock(side_effect=[first_response, second_response])

        async def fetch_first():
            return await client._get_result_csv_page("job-123")

        async def fetch_next(url):
            return await client._get_result_csv_by_url(url)

        result = await client._collect_pages(fetch_first, fetch_next)

        combined_csv = result.data.getvalue().decode()
        assert combined_csv == "id,name\n1,test\n2,test2\n"
        assert result.next_uri is None
        assert result.next_offset == 200
        assert first_response.raise_for_status.call_count == 1
        assert second_response.raise_for_status.call_count == 1
        assert first_response.release.call_count == 1
        assert second_response.release.call_count == 1


if __name__ == "__main__":
    aiounittest.main()
