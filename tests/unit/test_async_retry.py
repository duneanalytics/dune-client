"""Unit tests for AsyncDuneClient retry logic"""

from unittest.mock import AsyncMock, MagicMock, patch

import aiounittest
import pytest
from aiohttp import ClientError, ClientResponse, ClientResponseError

from dune_client.client_async import AsyncDuneClient


class TestAsyncRetryLogic(aiounittest.AsyncTestCase):
    """Test retry behavior in AsyncDuneClient"""

    def _mock_response(self, status=200, json_data=None):
        """Create a mock aiohttp response"""
        response = MagicMock(spec=ClientResponse)
        response.status = status
        response.json = AsyncMock(return_value=json_data or {"result": "success"})
        response.headers = {}
        response.read = AsyncMock(return_value=b"")
        response.release = MagicMock()
        return response

    def _create_client(self):
        """Create a client with mocked session (no real HTTP setup)"""
        client = AsyncDuneClient(api_key="test")
        client._retry_backoff = 0  # No delays in tests
        client._session = MagicMock()  # Mock session instead of creating real one
        client._session.request = AsyncMock()
        client._session.close = AsyncMock()
        return client

    async def test_retries_on_transient_failures(self):
        """Test retries on 429, 502-504, and ClientError"""
        test_cases = [
            (429, "rate limit"),
            (502, "bad gateway"),
            (503, "service unavailable"),
            (504, "gateway timeout"),
            (ClientError("network error"), "client error"),
        ]

        for failure, description in test_cases:
            client = self._create_client()
            success = self._mock_response(status=200)

            if isinstance(failure, Exception):
                client._session.request = AsyncMock(side_effect=[failure, success])
            else:
                error = self._mock_response(status=failure)
                client._session.request = AsyncMock(side_effect=[error, success])

            result = await client._get(route="/test")
            assert result == {"result": "success"}, f"Failed on {description}"
            assert client._session.request.call_count == 2

    async def test_no_retry_on_client_errors(self):
        """Test that 4xx errors (except 429) don't retry"""
        client = self._create_client()
        error = self._mock_response(status=400, json_data={"error": "bad request"})
        client._session.request = AsyncMock(return_value=error)

        result = await client._get(route="/test")
        assert "error" in result
        assert client._session.request.call_count == 1

    async def test_max_retries_then_returns_error(self):
        """Test that client gives up after max_attempts"""
        client = self._create_client()
        rate_limit = self._mock_response(status=429)
        client._session.request = AsyncMock(return_value=rate_limit)

        result = await client._get(route="/test", raw=True)
        assert result.status == 429
        assert client._session.request.call_count == client._max_attempts

        # Test with exceptions
        client = self._create_client()
        client._session.request = AsyncMock(side_effect=ClientError("persistent failure"))

        try:
            await client._get(route="/test")
            pytest.fail("Should have raised ClientError")
        except ClientError:
            assert client._session.request.call_count == client._max_attempts

    async def test_max_retries_raises_http_error_for_json_response(self):
        """Test that exhausted retries raise HTTP error even for JSON error responses"""
        client = self._create_client()
        # Mock a 429 response with JSON error body
        rate_limit = self._mock_response(status=429, json_data={"error": "rate limited"})
        # Make raise_for_status actually raise
        rate_limit.raise_for_status = MagicMock(
            side_effect=ClientResponseError(
                request_info=MagicMock(),
                history=(),
                status=429,
                message="Too Many Requests",
            )
        )
        client._session.request = AsyncMock(return_value=rate_limit)

        # Without raw=False (default), should raise HTTP error
        with pytest.raises(ClientResponseError) as exc_info:
            await client._get(route="/test")

        assert exc_info.value.status == 429
        assert client._session.request.call_count == client._max_attempts

    async def test_exponential_backoff(self):
        """Test retry delays double each attempt"""
        client = self._create_client()
        client._retry_backoff = 0.1  # Use small delay for this test

        rate_limit = self._mock_response(status=429)
        success = self._mock_response(status=200)
        client._session.request = AsyncMock(side_effect=[rate_limit, rate_limit, success])

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await client._get(route="/test")
            delays = [call.args[0] for call in mock_sleep.call_args_list]
            assert delays == [0.1, 0.2]  # Exponential backoff


if __name__ == "__main__":
    aiounittest.main()
