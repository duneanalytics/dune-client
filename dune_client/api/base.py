""" "
Basic Dune Client Class responsible for refreshing Dune Queries
Framework built on Dune's API Documentation
https://docs.dune.com/api-reference/overview/introduction
"""

from __future__ import annotations

import logging.config
import os
from json import JSONDecodeError
from typing import Any, Dict, List, Optional, Union, IO

from requests import Response, Session
from requests.adapters import HTTPAdapter, Retry

from dune_client.util import get_package_version

# Headers used for pagination in CSV results
DUNE_CSV_NEXT_URI_HEADER = "x-dune-next-uri"
DUNE_CSV_NEXT_OFFSET_HEADER = "x-dune-next-offset"
# Default maximum number of rows to retrieve per batch of results
MAX_NUM_ROWS_PER_BATCH = 32_000


# pylint: disable=too-few-public-methods
class BaseDuneClient:
    """
    A Base Client for Dune which sets up default values
    and provides some convenient functions to use in other clients
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        api_key: str,
        base_url: str = "https://api.dune.com",
        request_timeout: float = 10,
        client_version: str = "v1",
        performance: str = "medium",
    ):
        self.token = api_key
        self.base_url = base_url
        self.request_timeout = request_timeout
        self.client_version = client_version
        self.performance = performance
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
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
    def from_env(cls) -> BaseDuneClient:
        """
        Constructor allowing user to instantiate a client from environment variable
        without having to import dotenv or os manually
        We use `DUNE_API_KEY` as the environment variable that holds the API key.
        """
        return cls(
            api_key=os.environ["DUNE_API_KEY"],
            base_url=os.environ.get("DUNE_API_BASE_URL", "https://api.dune.com"),
            request_timeout=float(os.environ.get("DUNE_API_REQUEST_TIMEOUT", 10)),
        )

    @property
    def api_version(self) -> str:
        """Returns client version string"""
        return f"/api/{self.client_version}"

    def default_headers(self) -> Dict[str, str]:
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
        params: Optional[Dict[str, Union[str, int]]] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        allow_partial_results: str = "true",
    ) -> Dict[str, Union[str, int]]:
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
        ), "sampling cannot be combined with filters or pagination"

        params = params or {}
        params["allow_partial_results"] = allow_partial_results
        if columns is not None and len(columns) > 0:
            params["columns"] = ",".join(columns)
        if sample_count is not None:
            params["sample_count"] = sample_count
        if filters is not None:
            params["filters"] = filters
        if sort_by is not None and len(sort_by) > 0:
            params["sort_by"] = ",".join(sort_by)
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        return params


class BaseRouter(BaseDuneClient):
    """Extending the Base Client with elementary api routing"""

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

    def _route_url(self, route: Optional[str] = None, url: Optional[str] = None) -> str:
        if route is not None:
            final_url = f"{self.base_url}{self.api_version}{route}"
        elif url is not None:
            final_url = url
        else:
            assert route is not None or url is not None

        return final_url

    def _get(
        self,
        route: Optional[str] = None,
        params: Optional[Any] = None,
        raw: bool = False,
        url: Optional[str] = None,
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
        params: Optional[Any] = None,
        data: Optional[IO[bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
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
