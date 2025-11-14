"""
Uploads API endpoints for table management via /v1/uploads/*
This is the modern replacement for the legacy /table/* endpoints.
"""

from __future__ import annotations

from typing import IO

from dune_client.api.base import BaseRouter
from dune_client.models import (
    ClearTableResponse,
    CSVUploadResponse,
    DeleteTableResponse,
    DuneError,
    InsertDataResponse,
    UploadCreateResponse,
    UploadListResponse,
)


class UploadsAPI(BaseRouter):
    """
    Implementation of Uploads endpoints - Modern table management API
    https://docs.dune.com/api-reference/uploads/
    """

    def list_uploads(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> UploadListResponse:
        """
        https://docs.dune.com/api-reference/uploads/endpoint/list
        List all tables owned by the authenticated account.

        Args:
            limit: Maximum number of tables to return (max 10000)
            offset: Pagination offset

        Returns:
            UploadListResponse with list of tables and pagination info
        """
        response_json = self._get(
            route="/uploads",
            params={
                "limit": limit,
                "offset": offset,
            },
        )
        try:
            return UploadListResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "UploadListResponse", err) from err

    def create_table(
        self,
        namespace: str,
        table_name: str,
        schema: list[dict[str, str]],
        description: str = "",
        is_private: bool = False,
    ) -> UploadCreateResponse:
        """
        https://docs.dune.com/api-reference/uploads/endpoint/create
        Create an empty table with a specific schema.

        This endpoint consumes 10 credits per successful creation.

        Args:
            namespace: The namespace for the table
            table_name: The name of the table to create
            schema: List of column definitions, e.g. [{"name": "col1", "type": "varchar"}]
            description: Optional table description
            is_private: Whether the table should be private

        Returns:
            UploadCreateResponse with table details
        """
        result_json = self._post(
            route="/uploads",
            params={
                "namespace": namespace,
                "table_name": table_name,
                "schema": schema,
                "description": description,
                "is_private": is_private,
            },
        )
        try:
            return UploadCreateResponse.from_dict(result_json)
        except KeyError as err:
            raise DuneError(result_json, "UploadCreateResponse", err) from err

    def upload_csv(
        self,
        table_name: str,
        data: str,
        description: str = "",
        is_private: bool = False,
    ) -> CSVUploadResponse:
        """
        https://docs.dune.com/api-reference/uploads/endpoint/upload-csv
        Upload a CSV file to create a table with automatic schema inference.

        Limitations:
        - File must be < 200 MB
        - Storage limits vary by plan (1MB free, 15GB plus, 50GB premium)

        Args:
            table_name: The name of the table to create
            data: CSV data as a string
            description: Optional table description
            is_private: Whether the table should be private

        Returns:
            CSVUploadResponse with the created table name
        """
        response_json = self._post(
            route="/uploads/csv",
            params={
                "table_name": table_name,
                "data": data,
                "description": description,
                "is_private": is_private,
            },
        )
        try:
            return CSVUploadResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "CSVUploadResponse", err) from err

    def insert_data(
        self,
        namespace: str,
        table_name: str,
        data: IO[bytes],
        content_type: str,
    ) -> InsertDataResponse:
        """
        https://docs.dune.com/api-reference/uploads/endpoint/insert
        Insert data into an existing table.

        Supported content types:
        - text/csv
        - application/x-ndjson

        Args:
            namespace: The namespace of the table
            table_name: The name of the table
            data: File-like object containing the data to insert
            content_type: MIME type of the data (text/csv or application/x-ndjson)

        Returns:
            InsertDataResponse with rows/bytes written
        """
        result_json = self._post(
            route=f"/uploads/{namespace}/{table_name}/insert",
            headers={"Content-Type": content_type},
            data=data,
        )
        try:
            return InsertDataResponse.from_dict(result_json)
        except KeyError as err:
            raise DuneError(result_json, "InsertDataResponse", err) from err

    def clear_table(
        self,
        namespace: str,
        table_name: str,
    ) -> ClearTableResponse:
        """
        https://docs.dune.com/api-reference/uploads/endpoint/clear
        Remove all data from a table while preserving its structure and schema.

        Args:
            namespace: The namespace of the table
            table_name: The name of the table

        Returns:
            ClearTableResponse with confirmation message
        """
        result_json = self._post(route=f"/uploads/{namespace}/{table_name}/clear")
        try:
            return ClearTableResponse.from_dict(result_json)
        except KeyError as err:
            raise DuneError(result_json, "ClearTableResponse", err) from err

    def delete_table(
        self,
        namespace: str,
        table_name: str,
    ) -> DeleteTableResponse:
        """
        https://docs.dune.com/api-reference/uploads/endpoint/delete
        Permanently delete a table and all its data.

        Args:
            namespace: The namespace of the table
            table_name: The name of the table

        Returns:
            DeleteTableResponse with confirmation message
        """
        response_json = self._delete(route=f"/uploads/{namespace}/{table_name}")
        try:
            return DeleteTableResponse.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "DeleteTableResponse", err) from err
