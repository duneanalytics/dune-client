"""
Custom endpoints API enables users to
fetch and filter data from custom endpoints.
"""

from __future__ import annotations

from deprecated import deprecated

from dune_client.api.base import BaseRouter
from dune_client.models import (
    DuneError,
    ResultsResponse,
)


class CustomEndpointAPI(BaseRouter):
    """
    Custom endpoints API implementation.
    Methods:
        get_custom_endpoint_result(): returns the results of a custom endpoint.
    """

    @deprecated(
        version="1.8.1",
        reason="Custom endpoints feature is deprecated and will be removed in a future version",
    )
    def get_custom_endpoint_result(
        self,
        handle: str,
        endpoint: str,
        limit: int | None = None,
        offset: int | None = None,
        columns: list[str] | None = None,
        sample_count: int | None = None,
        filters: str | None = None,
        sort_by: list[str] | None = None,
    ) -> ResultsResponse:
        """
        Custom endpoints allow you to fetch and filter data from any
        custom endpoint you created.
        More information on Custom Endpoints can be round here:
        https://docs.dune.com/api-reference/custom/overview

        Args:
            handle (str): The handle of the team/user.
            endpoint (str): The slug of the custom endpoint.
            limit (int, optional): The maximum number of results to return.
            offset (int, optional): The number of results to skip.
            columns (List[str], optional): A list of columns to return.
            sample_count (int, optional): The number of results to return.
            filters (str, optional): The filters to apply.
            sort_by (List[str], optional): The columns to sort by.
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
