"""
Custom endpoints API enables users to
fetch and filter data from custom endpoints.
"""

from __future__ import annotations
from typing import List, Optional

from dune_client.api.base import BaseRouter
from dune_client.models import (
    DuneError,
    ResultsResponse,
)


class CustomEndpointAPI(BaseRouter):
    """
    
    """

    def get_custom_endpoint_result(
        self,
        handle: str,
        endpoint: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        columns: Optional[List[str]] = None,
        sample_count: Optional[int] = None,
        filters: Optional[str] = None,
        sort_by: Optional[List[str]] = None,
    ) -> ResultsResponse:
        """
        
        """
        params = self._build_parameters(
            columns=columns,
            sample_count=sample_count,
            filters=filters,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
        )
        response_json = self._get(
            route=f"/endpoints/{handle}/{endpoint}/results",
            params=params,
        )
        try:
            return ResultsResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "ResultsResponse", err) from err


