""" "
Basic Dune Client Class responsible for refreshing Dune Queries
Framework built on Dune's API Documentation
https://docs.dune.com/api-reference/overview/introduction
"""

from __future__ import annotations

import logging.config
import os
from json import JSONDecodeError
from typing import IO, TYPE_CHECKING, Any

from deprecated import deprecated
from requests import Response, Session
from requests.adapters import HTTPAdapter, Retry

from dune_client.util import get_package_version

if TYPE_CHECKING:
    from dune_client.types import QueryParameters

# Headers used for pagination in CSV results
DUNE_CSV_NEXT_URI_HEADER = "x-dune-next-uri"
DUNE_CSV_NEXT_OFFSET_HEADER = "x-dune-next-offset"
# Default maximum number of rows to retrieve per batch of results
MAX_NUM_ROWS_PER_BATCH = 32_000


class BaseDuneClient:
    """
    A Base Client for Dune which sets up default values
    and provides some convenient functions to use in other clients
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        request_timeout: float | None = None,
        client_version: str = "v1",
        performance: str = "medium",
    ):
        # Read from environment variables if not provided
        api_key = api_key or os.environ["DUNE_API_KEY"]
        base_url = base_url or os.environ.get("DUNE_API_BASE_URL", "https://api.dune.com")
        request_timeout = request_timeout or float(os.environ.get("DUNE_API_REQUEST_TIMEOUT", "10"))

        self.token = api_key
        self.base_url = base_url
        self.request_timeout = request_timeout
        self.client_version = client_version
        self.performance = performance
        self.logger = logging.getLogger(__name__)
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist={429, 502, 503, 504},
            allowed_methods={"GET", "POST", "PATCH"},
            raise_on_status=True,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http = Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

    @classmethod
    @deprecated(
        version="1.8.0",
        reason="Use DuneClient() without any arguments instead, which will automatically read from environment variables",
    )
    def from_env(cls) -> BaseDuneClient:
        """
        Constructor allowing user to instantiate a client from environment variable
        without having to import dotenv or os manually
        We use `DUNE_API_KEY` as the environment variable that holds the API key.
        """
        return cls()

    @property
    def api_version(self) -> str:
        """Returns client version string"""
        return f"/api/{self.client_version}"

    def default_headers(self) -> dict[str, str]:
        """Return default headers containing Dune Api token"""
        client_version = get_package_version("dune-client") or "1.3.0"
        return {
            "x-dune-api-key": self.token,
            "User-Agent": f"dune-client/{client_version} (https://pypi.org/project/dune-client/)",
        }

    ############
    # Utilities:
    ############

    def _build_parameters(
        self,
        params: QueryParameters | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        allow_partial_results: str = "true",
    ) -> QueryParameters:
        """
        Utility function that builds a dictionary of parameters to be used
        when retrieving advanced results (filters, pagination, sorting, etc.).
        This is shared between the sync and async client.
        """
        # Ensure we don't specify parameters that are incompatible:
        assert (
            # We are not sampling
            sample_count is None
            # We are sampling and don't use filters or pagination
            or (limit is None and offset is None and filters is None)
        ), "sapling cannot be combined with filters or pagination"

        result: QueryParameters = dict(params) if params else {}
        result["allow_partial_results"] = allow_partial_results
        if columns:
            result["columns"] = ",".join(columns)
        if sample_count is not None:
            result["sample_count"] = sample_count
        if filters is not None:
            result["filters"] = filters
        if sort_by:
            result["sort_by"] = ",".join(sort_by)
        if limit is not None:
            result["limit"] = limit
        if offset is not None:
            result["offset"] = offset

        return result


class BaseRouter(BaseDuneClient):
    """Extending the Base Client with elementary api routing"""

    def _handle_response(self, response: Response) -> Any:
        """Generic response handler utilized by all Dune API routes"""
        try:
            # Some responses can be decoded and converted to DuneErrors
            response_json = response.json()
            self.logger.debug(f"received response {response_json}")
        except JSONDecodeError as err:
            # Others can't. Only raise HTTP error for not decodable errors
            response.raise_for_status()
            raise ValueError("Unreachable since previous line raises") from err
        else:
            # Check status code even for valid JSON responses
            # Error responses (4xx, 5xx) can have JSON bodies
            response.raise_for_status()
            return response_json

    def _route_url(self, route: str | None = None, url: str | None = None) -> str:
        if route is not None:
            final_url = f"{self.base_url}{self.api_version}{route}"
        elif url is not None:
            final_url = url
        else:
            assert route is not None or url is not None

        return final_url

    def _get(
        self,
        route: str | None = None,
        params: Any | None = None,
        raw: bool = False,
        url: str | None = None,
    ) -> Any:
        """Generic interface for the GET method of a Dune API request"""
        final_url = self._route_url(route=route, url=url)
        self.logger.debug(f"GET received input url={final_url}")

        response = self.http.get(
            url=final_url,
            headers=self.default_headers(),
            timeout=self.request_timeout,
            params=params,
        )
        if raw:
            return response
        return self._handle_response(response)

    def _post(
        self,
        route: str,
        params: Any | None = None,
        data: IO[bytes] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Generic interface for the POST method of a Dune API request"""
        url = self._route_url(route)
        self.logger.debug(f"POST received input url={url}, params={params}")
        response = self.http.post(
            url=url,
            json=params,
            headers=dict(self.default_headers(), **headers if headers else {}),
            timeout=self.request_timeout,
            data=data,
        )
        return self._handle_response(response)

    def _patch(self, route: str, params: Any) -> Any:
        """Generic interface for the PATCH method of a Dune API request"""
        url = self._route_url(route)
        self.logger.debug(f"PATCH received input url={url}, params={params}")
        response = self.http.patch(
            url=url,
            json=params,
            headers=self.default_headers(),
            timeout=self.request_timeout,
        )
        return self._handle_response(response)

    def _delete(self, route: str) -> Any:
        """Generic interface for the DELETE method of a Dune API request"""
        url = self._route_url(route)
        self.logger.debug(f"DELETE received input url={url}")
        response = self.http.delete(
            url=url,
            headers=self.default_headers(),
            timeout=self.request_timeout,
        )
        return self._handle_response(response)
