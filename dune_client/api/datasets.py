"""
Datasets API endpoints for dataset discovery via /v1/datasets/*
"""

from __future__ import annotations

from dune_client.api.base import BaseRouter
from dune_client.models import DatasetListResponse, DatasetResponse, DuneError


class DatasetsAPI(BaseRouter):
    """
    Implementation of Datasets endpoints
    https://docs.dune.com/api-reference/datasets/
    """

    def list_datasets(
        self,
        limit: int = 50,
        offset: int = 0,
        owner_handle: str | None = None,
        type: str | None = None,
    ) -> DatasetListResponse:
        """
        https://docs.dune.com/api-reference/datasets/endpoint/list
        Retrieve a paginated list of datasets with optional filtering.

        Args:
            limit: Maximum number of datasets to return (max 250)
            offset: Pagination offset
            owner_handle: Optional filter by owner handle
            type: Optional filter by dataset type (transformation_view, transformation_table,
                  uploaded_table, decoded_table, spell, dune_table)

        Returns:
            DatasetListResponse with list of datasets and total count
        """
        params: dict[str, int | str] = {
            "limit": limit,
            "offset": offset,
        }
        if owner_handle is not None:
            params["owner_handle"] = owner_handle
        if type is not None:
            params["type"] = type

        response_json = self._get(
            route="/datasets",
            params=params,
        )
        try:
            return DatasetListResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "DatasetListResponse", err) from err

    def get_dataset(self, full_name: str) -> DatasetResponse:
        """
        https://docs.dune.com/api-reference/datasets/endpoint/get
        Retrieve detailed information about a specific dataset.

        Args:
            full_name: The dataset full name (e.g., 'dune.shozaib_khan.aarna')

        Returns:
            DatasetResponse with full dataset details including columns and metadata
        """
        response_json = self._get(route=f"/datasets/{full_name}")
        try:
            return DatasetResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "DatasetResponse", err) from err
