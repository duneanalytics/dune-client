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
        result = self.dune.list_datasets(limit=10, offset=0)

        self.assertIsInstance(result, DatasetListResponse)
        self.assertIsInstance(result.datasets, list)
        self.assertIsInstance(result.total, int)
        self.assertGreater(result.total, 0)

        if len(result.datasets) > 0:
            dataset = result.datasets[0]
            self.assertIsNotNone(dataset.slug)
            self.assertIsNotNone(dataset.name)
            self.assertIsNotNone(dataset.type)
            self.assertIsNotNone(dataset.owner)
            self.assertIsNotNone(dataset.namespace)

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
        result = self.dune.get_dataset("dex.trades")

        self.assertIsInstance(result, DatasetResponse)
        self.assertEqual(result.slug, "dex.trades")
        self.assertIsNotNone(result.name)
        self.assertIsNotNone(result.type)
        self.assertIsNotNone(result.owner)
        self.assertIsNotNone(result.namespace)
        self.assertIsNotNone(result.columns)
        self.assertIsInstance(result.columns, list)
        self.assertGreater(len(result.columns), 0)

        column = result.columns[0]
        self.assertIsNotNone(column.name)
        self.assertIsNotNone(column.type)

    def test_get_dataset_with_uploaded_table(self):
        result_list = self.dune.list_datasets(
            limit=1,
            type="uploaded_table",
        )

        if len(result_list.datasets) > 0:
            slug = result_list.datasets[0].slug
            result = self.dune.get_dataset(slug)

            self.assertIsInstance(result, DatasetResponse)
            self.assertEqual(result.slug, slug)
            self.assertEqual(result.type, "uploaded_table")


if __name__ == "__main__":
    unittest.main()
