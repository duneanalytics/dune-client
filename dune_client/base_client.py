""""
Basic Dune Client Class responsible for refreshing Dune Queries
Framework built on Dune's API Documentation
https://duneanalytics.notion.site/API-Documentation-1b93d16e0fa941398e15047f643e003a
"""
from __future__ import annotations

import logging.config
import os
from typing import Dict


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
        return {"x-dune-api-key": self.token}
