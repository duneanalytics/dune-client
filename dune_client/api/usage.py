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
        start_date: str,
        end_date: str,
    ) -> UsageResponse:
        """
        Get credits usage data, overage, number of private queries run, storage, etc.
        over a specific timeframe.
        https://docs.dune.com/api-reference/usage/endpoint/get-usage

        Args:
            start_date: Start date for the usage period (ISO format: YYYY-MM-DD)
            end_date: End date for the usage period (ISO format: YYYY-MM-DD)

        Returns:
            UsageResponse containing usage statistics

        Requires Plus subscription!
        """
        params = {
            "start_date": start_date,
            "end_date": end_date,
        }
        response_json = self._get(
            route="/usage",
            params=params,
        )
        try:
            return UsageResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "UsageResponse", err) from err

