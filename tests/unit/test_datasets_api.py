import unittest
from unittest.mock import MagicMock

from dune_client.api.datasets import DatasetsAPI
from dune_client.models import (
    DatasetListResponse,
    DatasetResponse,
)


class TestDatasetsAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.api = DatasetsAPI(api_key="test_key")
        self.api._get = MagicMock()

    def test_list_datasets_minimal(self):
        mock_response = {
            "datasets": [
                {
                    "slug": "dex.trades",
                    "name": "DEX Trades",
                    "type": "transformation_view",
                    "owner": {"id": 1, "handle": "dune"},
                    "namespace": "dex",
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-02T00:00:00Z",
                    "is_private": False,
                }
            ],
            "total": 1,
        }
        self.api._get.return_value = mock_response

        result = self.api.list_datasets(limit=50, offset=0)

        self.api._get.assert_called_once_with(
            route="/v1/datasets",
            params={"limit": 50, "offset": 0},
        )
        self.assertIsInstance(result, DatasetListResponse)
        self.assertEqual(len(result.datasets), 1)
        self.assertEqual(result.datasets[0].slug, "dex.trades")
        self.assertEqual(result.total, 1)

    def test_list_datasets_with_filters(self):
        mock_response = {
            "datasets": [
                {
                    "slug": "user.my_table",
                    "name": "My Table",
                    "type": "uploaded_table",
                    "owner": {"id": 123, "handle": "test_user"},
                    "namespace": "user",
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-02T00:00:00Z",
                    "is_private": True,
                }
            ],
            "total": 1,
        }
        self.api._get.return_value = mock_response

        result = self.api.list_datasets(
            limit=100,
            offset=10,
            owner_handle="test_user",
            type="uploaded_table",
        )

        self.api._get.assert_called_once_with(
            route="/v1/datasets",
            params={
                "limit": 100,
                "offset": 10,
                "owner_handle": "test_user",
                "type": "uploaded_table",
            },
        )
        self.assertIsInstance(result, DatasetListResponse)
        self.assertEqual(result.datasets[0].type, "uploaded_table")
        self.assertEqual(result.datasets[0].owner.handle, "test_user")

    def test_get_dataset(self):
        mock_response = {
            "slug": "dex.trades",
            "name": "DEX Trades",
            "type": "transformation_view",
            "owner": {"id": 1, "handle": "dune"},
            "namespace": "dex",
            "columns": [
                {"name": "block_time", "type": "timestamp"},
                {"name": "token_bought_address", "type": "varchar"},
                {"name": "token_sold_address", "type": "varchar"},
                {"name": "amount_usd", "type": "double"},
            ],
            "description": "All DEX trades across multiple chains",
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "is_private": False,
        }
        self.api._get.return_value = mock_response

        result = self.api.get_dataset("dex.trades")

        self.api._get.assert_called_once_with(route="/v1/datasets/dex.trades")
        self.assertIsInstance(result, DatasetResponse)
        self.assertEqual(result.slug, "dex.trades")
        self.assertEqual(result.name, "DEX Trades")
        self.assertEqual(len(result.columns), 4)
        self.assertEqual(result.columns[0].name, "block_time")
        self.assertEqual(result.columns[0].type, "timestamp")
        self.assertEqual(result.description, "All DEX trades across multiple chains")

    def test_get_dataset_no_description(self):
        mock_response = {
            "slug": "test.dataset",
            "name": "Test Dataset",
            "type": "uploaded_table",
            "owner": {"id": 123, "handle": "test_user"},
            "namespace": "test",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": "varchar"},
            ],
            "description": None,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "is_private": True,
        }
        self.api._get.return_value = mock_response

        result = self.api.get_dataset("test.dataset")

        self.assertIsInstance(result, DatasetResponse)
        self.assertIsNone(result.description)
        self.assertTrue(result.is_private)


if __name__ == "__main__":
    unittest.main()
