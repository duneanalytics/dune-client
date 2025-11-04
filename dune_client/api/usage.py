"""
Usage API endpoints enable users to retrieve usage and billing information.
https://docs.dune.com/api-reference/usage/endpoint/get-usage
"""

from __future__ import annotations

from dune_client.api.base import BaseRouter
from dune_client.models import DuneError, UsageResponse


class UsageAPI(BaseRouter):
    """
    Implementation of Usage endpoints - Plus subscription only
    https://docs.dune.com/api-reference/usage/
    """

    def get_usage(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> UsageResponse:
        """
        Get credits usage data, billing periods, storage, etc.
        https://docs.dune.com/api-reference/usage/endpoint/get-usage

        Args:
            start_date: Optional start date for the usage period (format: YYYY-MM-DD)
            end_date: Optional end date for the usage period (format: YYYY-MM-DD)

        Returns:
            UsageResponse containing usage statistics and billing periods

        Requires Plus subscription!
        """
        payload = {}
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date

        response_json = self._post(
            route="/usage",
            params=payload,
        )
        try:
            return UsageResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "UsageResponse", err) from err
