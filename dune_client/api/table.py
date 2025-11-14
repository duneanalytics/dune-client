"""
Table API endpoints enables users to
create and insert data into Dune.

DEPRECATED: This API uses legacy /table/* routes.
Please use UploadsAPI for the modern /v1/uploads/* endpoints instead.
"""

from __future__ import annotations

from typing import IO

from deprecated import deprecated

from dune_client.api.base import BaseRouter
from dune_client.models import (
    ClearTableResult,
    CreateTableResult,
    DeleteTableResult,
    DuneError,
    InsertTableResult,
)


class TableAPI(BaseRouter):
    """
    Implementation of Table endpoints - Plus subscription only
    https://docs.dune.com/api-reference/tables/

    DEPRECATED: This API uses legacy /table/* routes.
    Please use UploadsAPI for the modern /v1/uploads/* endpoints instead.
    """

    @deprecated(
        version="1.9.0",
        reason="Use UploadsAPI.upload_csv() instead. This method uses legacy /table/* routes.",
    )
    def upload_csv(
        self,
        table_name: str,
        data: str,
        description: str = "",
        is_private: bool = False,
    ) -> bool:
        """
        https://docs.dune.com/api-reference/tables/endpoint/upload
        This endpoint allows you to upload any .csv file into Dune. The only limitations are:

        - File has to be < 200 MB
        - Column names in the table can't start with a special character or digits.
        - Private uploads require a Plus subscription.

        Below are the specifics of how to work with the API.
        """
        response_json = self._post(
            route="/table/upload/csv",
            params={
                "table_name": table_name,
                "description": description,
                "data": data,
                "is_private": is_private,
            },
        )
        try:
            return bool(response_json["success"])
        except KeyError as err:
            raise DuneError(response_json, "UploadCsvResponse", err) from err

    @deprecated(
        version="1.9.0",
        reason="Use UploadsAPI.create_table() instead. This method uses legacy /table/* routes.",
    )
    def create_table(
        self,
        namespace: str,
        table_name: str,
        schema: list[dict[str, str]],
        description: str = "",
        is_private: bool = False,
    ) -> CreateTableResult:
        """
        https://docs.dune.com/api-reference/tables/endpoint/create
        The create table endpoint allows you to create an empty table
        with a specific schema in Dune.

        The only limitations are:
        - If a table already exists with the same name, the request will fail.
        - Column names in the table can't start with a special character or a digit.
        """

        result_json = self._post(
            route="/table/create",
            params={
                "namespace": namespace,
                "table_name": table_name,
                "schema": schema,
                "description": description,
                "is_private": is_private,
            },
        )
        try:
            return CreateTableResult.from_dict(result_json)
        except KeyError as err:
            raise DuneError(result_json, "CreateTableResult", err) from err

    @deprecated(
        version="1.9.0",
        reason="Use UploadsAPI.insert_data() instead. This method uses legacy /table/* routes.",
    )
    def insert_table(
        self,
        namespace: str,
        table_name: str,
        data: IO[bytes],
        content_type: str,
    ) -> InsertTableResult:
        """
        https://docs.dune.com/api-reference/tables/endpoint/insert
        The insert table endpoint allows you to insert data into an existing table in Dune.

        The only limitations are:
        - The file has to be in json or csv format
        - The file has to have the same schema as the table
        """

        result_json = self._post(
            route=f"/table/{namespace}/{table_name}/insert",
            headers={"Content-Type": content_type},
            data=data,
        )
        try:
            return InsertTableResult.from_dict(result_json)
        except KeyError as err:
            raise DuneError(result_json, "InsertTableResult", err) from err

    @deprecated(
        version="1.9.0",
        reason="Use UploadsAPI.clear_table() instead. This method uses legacy /table/* routes.",
    )
    def clear_data(self, namespace: str, table_name: str) -> ClearTableResult:
        """
        https://docs.dune.com/api-reference/tables/endpoint/clear
        The Clear endpoint removes all the data in the specified table,
        but does not delete the table.
        """

        result_json = self._post(route=f"/table/{namespace}/{table_name}/clear")
        try:
            return ClearTableResult.from_dict(result_json)
        except KeyError as err:
            raise DuneError(result_json, "ClearTableResult", err) from err

    @deprecated(
        version="1.9.0",
        reason="Use UploadsAPI.delete_table() instead. This method uses legacy /table/* routes.",
    )
    def delete_table(self, namespace: str, table_name: str) -> DeleteTableResult:
        """
        https://docs.dune.com/api-reference/tables/endpoint/delete
        The delete table endpoint allows you to delete an existing table in Dune.
        """

        response_json = self._delete(route=f"/table/{namespace}/{table_name}")
        try:
            return DeleteTableResult.from_dict(response_json)
        except KeyError as err:
            raise DuneError(response_json, "DeleteTableResult", err) from err
