import os
import unittest
from io import BytesIO

import pytest

from dune_client.client import DuneClient
from dune_client.models import (
    ClearTableResponse,
    CSVUploadResponse,
    DeleteTableResponse,
    InsertDataResponse,
    UploadCreateResponse,
    UploadListResponse,
)


@pytest.mark.e2e
class TestUploadsIntegration(unittest.TestCase):
    """
    E2E tests for UploadsAPI endpoints.
    These tests require a valid DUNE_API_KEY and Plus subscription.
    """

    def setUp(self) -> None:
        self.dune = DuneClient()
        self.test_namespace = os.getenv("DUNE_API_KEY_OWNER_HANDLE", "test")
        self.test_table_name = f"test_uploads_api_{int(__import__('time').time())}"

    def test_create_and_delete_table(self):
        schema = [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "varchar"},
            {"name": "value", "type": "double"},
        ]

        result = self.dune.create_table(
            namespace=self.test_namespace,
            table_name=self.test_table_name,
            schema=schema,
            description="Test table created by E2E test",
            is_private=True,
        )

        self.assertIsInstance(result, UploadCreateResponse)
        self.assertEqual(result.table_name, self.test_table_name)
        self.assertEqual(result.namespace, self.test_namespace)

        delete_result = self.dune.delete_table(
            namespace=self.test_namespace,
            table_name=self.test_table_name,
        )
        self.assertIsInstance(delete_result, DeleteTableResponse)

    def test_upload_csv_and_delete(self):
        csv_data = """id,name,value
1,Alice,10.5
2,Bob,20.3
3,Charlie,15.7
"""

        result = self.dune.upload_csv(
            table_name=self.test_table_name,
            data=csv_data,
            description="CSV uploaded by E2E test",
            is_private=True,
        )

        self.assertIsInstance(result, CSVUploadResponse)
        self.assertEqual(result.table_name, self.test_table_name)

        delete_result = self.dune.delete_table(
            namespace=self.test_namespace,
            table_name=f"dataset_{self.test_table_name}",
        )
        self.assertIsInstance(delete_result, DeleteTableResponse)

    def test_list_uploads(self):
        result = self.dune.list_uploads(limit=10, offset=0)

        self.assertIsInstance(result, UploadListResponse)
        self.assertIsInstance(result.tables, list)

    def test_full_table_lifecycle(self):
        schema = [
            {"name": "id", "type": "integer"},
            {"name": "message", "type": "varchar"},
        ]

        create_result = self.dune.create_table(
            namespace=self.test_namespace,
            table_name=self.test_table_name,
            schema=schema,
            description="Full lifecycle test",
            is_private=True,
        )
        self.assertIsInstance(create_result, UploadCreateResponse)

        csv_data = b"id,message\n1,Hello\n2,World\n"
        insert_result = self.dune.insert_data(
            namespace=self.test_namespace,
            table_name=self.test_table_name,
            data=BytesIO(csv_data),
            content_type="text/csv",
        )
        self.assertIsInstance(insert_result, InsertDataResponse)
        self.assertEqual(insert_result.rows_written, 2)

        clear_result = self.dune.clear_table(
            namespace=self.test_namespace,
            table_name=self.test_table_name,
        )
        self.assertIsInstance(clear_result, ClearTableResponse)

        delete_result = self.dune.delete_table(
            namespace=self.test_namespace,
            table_name=self.test_table_name,
        )
        self.assertIsInstance(delete_result, DeleteTableResponse)


if __name__ == "__main__":
    unittest.main()
