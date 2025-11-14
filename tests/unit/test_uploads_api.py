import unittest
from io import BytesIO
from unittest.mock import MagicMock

from dune_client.api.uploads import UploadsAPI
from dune_client.models import (
    ClearTableResponse,
    CSVUploadResponse,
    DeleteTableResponse,
    InsertDataResponse,
    UploadCreateResponse,
    UploadListResponse,
)


class TestUploadsAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.api = UploadsAPI(api_key="test_key")
        self.api._get = MagicMock()
        self.api._post = MagicMock()
        self.api._delete = MagicMock()

    def test_list_uploads(self):
        mock_response = {
            "tables": [
                {
                    "full_name": "dune.test_namespace.test_table",
                    "is_private": False,
                    "table_size_bytes": "1024",
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-02T00:00:00Z",
                    "owner": {"handle": "test_user", "type": "user"},
                    "columns": [
                        {"name": "col1", "type": "varchar", "nullable": False},
                        {"name": "col2", "type": "integer", "nullable": False},
                    ],
                }
            ],
            "next_offset": 50,
        }
        self.api._get.return_value = mock_response

        result = self.api.list_uploads(limit=50, offset=0)

        self.api._get.assert_called_once_with(
            route="/uploads",
            params={"limit": 50, "offset": 0},
        )
        self.assertIsInstance(result, UploadListResponse)
        self.assertEqual(len(result.tables), 1)
        self.assertEqual(result.tables[0].full_name, "dune.test_namespace.test_table")
        self.assertEqual(result.next_offset, 50)

    def test_create_table(self):
        mock_response = {
            "namespace": "test_namespace",
            "table_name": "test_table",
            "full_name": "test_namespace.test_table",
            "example_query": "SELECT * FROM test_namespace.test_table",
        }
        self.api._post.return_value = mock_response

        schema = [{"name": "col1", "type": "varchar"}, {"name": "col2", "type": "int"}]
        result = self.api.create_table(
            namespace="test_namespace",
            table_name="test_table",
            schema=schema,
            description="Test description",
            is_private=False,
        )

        self.api._post.assert_called_once_with(
            route="/uploads",
            params={
                "namespace": "test_namespace",
                "table_name": "test_table",
                "schema": schema,
                "description": "Test description",
                "is_private": False,
            },
        )
        self.assertIsInstance(result, UploadCreateResponse)
        self.assertEqual(result.table_name, "test_table")
        self.assertEqual(result.namespace, "test_namespace")

    def test_upload_csv(self):
        mock_response = {
            "table_name": "test_table",
        }
        self.api._post.return_value = mock_response

        csv_data = "col1,col2\nval1,val2\n"
        result = self.api.upload_csv(
            table_name="test_table",
            data=csv_data,
            description="Test CSV",
            is_private=True,
        )

        self.api._post.assert_called_once_with(
            route="/uploads/csv",
            params={
                "table_name": "test_table",
                "data": csv_data,
                "description": "Test CSV",
                "is_private": True,
            },
        )
        self.assertIsInstance(result, CSVUploadResponse)
        self.assertEqual(result.table_name, "test_table")

    def test_insert_data(self):
        mock_response = {
            "rows_written": 100,
            "bytes_written": 2048,
            "name": "dune.test_namespace.test_table",
        }
        self.api._post.return_value = mock_response

        data = BytesIO(b"col1,col2\nval1,val2\n")
        result = self.api.insert_data(
            namespace="test_namespace",
            table_name="test_table",
            data=data,
            content_type="text/csv",
        )

        self.api._post.assert_called_once_with(
            route="/uploads/test_namespace/test_table/insert",
            headers={"Content-Type": "text/csv"},
            data=data,
        )
        self.assertIsInstance(result, InsertDataResponse)
        self.assertEqual(result.rows_written, 100)
        self.assertEqual(result.bytes_written, 2048)
        self.assertEqual(result.name, "dune.test_namespace.test_table")

    def test_clear_table(self):
        mock_response = {
            "message": "Table cleared successfully",
        }
        self.api._post.return_value = mock_response

        result = self.api.clear_table(
            namespace="test_namespace",
            table_name="test_table",
        )

        self.api._post.assert_called_once_with(route="/uploads/test_namespace/test_table/clear")
        self.assertIsInstance(result, ClearTableResponse)
        self.assertEqual(result.message, "Table cleared successfully")

    def test_delete_table(self):
        mock_response = {
            "message": "Table deleted successfully",
        }
        self.api._delete.return_value = mock_response

        result = self.api.delete_table(
            namespace="test_namespace",
            table_name="test_table",
        )

        self.api._delete.assert_called_once_with(route="/uploads/test_namespace/test_table")
        self.assertIsInstance(result, DeleteTableResponse)
        self.assertEqual(result.message, "Table deleted successfully")


if __name__ == "__main__":
    unittest.main()
