import unittest

import pytest

from dune_client.client import DuneClient
from dune_client.models import DatasetListResponse, DatasetResponse


@pytest.mark.e2e
class TestDatasetsIntegration(unittest.TestCase):
    """
    E2E tests for DatasetsAPI endpoints.
    These tests require a valid DUNE_API_KEY.
    """

    def setUp(self) -> None:
        self.dune = DuneClient()

    def test_list_datasets(self):
        result = self.dune.list_datasets(limit=10, offset=0, type="uploaded_table")

        self.assertIsInstance(result, DatasetListResponse)
        self.assertIsInstance(result.datasets, list)
        self.assertIsInstance(result.total, int)
        self.assertGreater(result.total, 0)

        if len(result.datasets) > 0:
            dataset = result.datasets[0]
            self.assertIsNotNone(dataset.full_name)
            self.assertIsNotNone(dataset.type)
            self.assertIsNotNone(dataset.owner)
            self.assertIsNotNone(dataset.columns)

    def test_list_datasets_with_filters(self):
        result = self.dune.list_datasets(
            limit=5,
            offset=0,
            type="transformation_view",
        )

        self.assertIsInstance(result, DatasetListResponse)
        self.assertIsInstance(result.datasets, list)

        for dataset in result.datasets:
            self.assertEqual(dataset.type, "transformation_view")

    def test_list_datasets_by_owner(self):
        result = self.dune.list_datasets(
            limit=5,
            offset=0,
            owner_handle="dune",
        )

        self.assertIsInstance(result, DatasetListResponse)
        self.assertIsInstance(result.datasets, list)

        for dataset in result.datasets:
            self.assertEqual(dataset.owner.handle, "dune")

    def test_get_dataset(self):
        result_list = self.dune.list_datasets(limit=1, type="uploaded_table")
        if len(result_list.datasets) == 0:
            self.skipTest("No uploaded tables found to test")

        full_name = result_list.datasets[0].full_name
        result = self.dune.get_dataset(full_name)

        self.assertIsInstance(result, DatasetResponse)
        self.assertEqual(result.full_name, full_name)
        self.assertIsNotNone(result.type)
        self.assertIsNotNone(result.owner)
        self.assertIsNotNone(result.columns)
        self.assertIsInstance(result.columns, list)
        self.assertGreater(len(result.columns), 0)

        column = result.columns[0]
        self.assertIsNotNone(column.name)
        self.assertIsNotNone(column.type)
        self.assertIsNotNone(column.nullable)

    def test_get_dataset_with_uploaded_table(self):
        result_list = self.dune.list_datasets(
            limit=1,
            type="uploaded_table",
        )

        if len(result_list.datasets) > 0:
            full_name = result_list.datasets[0].full_name
            result = self.dune.get_dataset(full_name)

            self.assertIsInstance(result, DatasetResponse)
            self.assertEqual(result.full_name, full_name)
            self.assertEqual(result.type, "uploaded_table")


if __name__ == "__main__":
    unittest.main()
