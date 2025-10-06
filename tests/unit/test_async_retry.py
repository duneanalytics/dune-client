"""Unit tests for AsyncDuneClient retry logic"""

from unittest.mock import AsyncMock, MagicMock

import aiounittest
import httpx
import pytest

from dune_client.client_async import AsyncDuneClient


class TestAsyncRetryLogic(aiounittest.AsyncTestCase):
    """Test retry behavior in AsyncDuneClient (now handled by httpx transport)"""

    def _mock_response(self, status=200, json_data=None):
        """Create a mock httpx response"""
        response = MagicMock(spec=httpx.Response)
        response.status_code = status
        response.json = MagicMock(return_value=json_data or {"result": "success"})
        response.headers = {}
        response.content = b""
        response.raise_for_status = MagicMock()
        return response

    def _create_client(self):
        """Create a client with mocked session (no real HTTP setup)"""
        client = AsyncDuneClient(api_key="test")
        client._session = MagicMock()  # Mock session instead of creating real one
        client._session.request = AsyncMock()
        client._session.close = AsyncMock()
        return client

    async def test_successful_request(self):
        """Test successful HTTP request with httpx"""
        client = self._create_client()
        success = self._mock_response(status=200)
        client._session.request = AsyncMock(return_value=success)

        result = await client._get(route="/test")
        assert result == {"result": "success"}
        assert client._session.request.call_count == 1

    async def test_error_response_handling(self):
        """Test that error responses are handled correctly"""
        client = self._create_client()
        error = self._mock_response(status=400, json_data={"error": "bad request"})
        client._session.request = AsyncMock(return_value=error)

        result = await client._get(route="/test")
        assert "error" in result
        assert client._session.request.call_count == 1

    async def test_http_error_raises(self):
        """Test that HTTP errors raise exceptions when raise_for_status is called"""
        client = self._create_client()
        error = self._mock_response(status=429, json_data={"error": "rate limited"})
        error.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "Too Many Requests",
                request=MagicMock(),
                response=error,
            )
        )
        client._session.request = AsyncMock(return_value=error)

        with pytest.raises(httpx.HTTPStatusError):
            await client._get(route="/test")

    async def test_raw_response_mode(self):
        """Test that raw=True returns the response object directly"""
        client = self._create_client()
        response = self._mock_response(status=200)
        client._session.request = AsyncMock(return_value=response)

        result = await client._get(route="/test", raw=True)
        assert result == response
        # raw mode should not call response.json()
        response.json.assert_not_called()


if __name__ == "__main__":
    aiounittest.main()
