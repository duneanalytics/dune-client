import unittest

import aiounittest
import pandas as pd

from dune_client.client_async import AsyncDuneClient
from dune_client.query import QueryBase


class TestDuneClient(aiounittest.AsyncTestCase):
    def setUp(self) -> None:
        self.query = QueryBase(name="Sample Query", query_id=1215383)
        self.multi_rows_query = QueryBase(
            name="Query that returns multiple rows",
            query_id=3435763,
        )

    async def test_disconnect(self):
        dune = AsyncDuneClient()
        await dune.connect()
        results = (await dune.run_query(self.query)).get_rows()
        assert len(results) > 0
        await dune.disconnect()
        assert dune._session is None

    async def test_run_query_context_manager_singleton(self):
        dune = AsyncDuneClient()
        async with dune as cl:
            results = (await cl.run_query(self.query)).get_rows()
        assert len(results) > 0

    async def test_run_query_context_manager(self):
        async with AsyncDuneClient() as cl:
            results = (await cl.run_query(self.query)).get_rows()
        assert len(results) > 0

    async def test_run_query_with_pagination(self):
        # Arrange
        async with AsyncDuneClient() as cl:
            # Act
            results = (await cl.run_query(self.multi_rows_query, batch_size=1)).get_rows()

        # Assert
        assert results == [
            {"number": 1},
            {"number": 2},
            {"number": 3},
            {"number": 4},
            {"number": 5},
        ]

    async def test_run_query_with_filters(self):
        # Arrange
        async with AsyncDuneClient() as cl:
            # Act
            results = (await cl.run_query(self.multi_rows_query, filters="number < 3")).get_rows()

        # Assert
        assert results == [{"number": 1}, {"number": 2}]

    async def test_run_query_csv_with_pagination(self):
        # Arrange
        async with AsyncDuneClient() as cl:
            # Act
            result_csv = await cl.run_query_csv(self.multi_rows_query, batch_size=1)

        # Assert
        assert pd.read_csv(result_csv.data).to_dict(orient="records") == [
            {"number": 1},
            {"number": 2},
            {"number": 3},
            {"number": 4},
            {"number": 5},
        ]

    async def test_run_query_csv_with_filters(self):
        # Arrange
        async with AsyncDuneClient() as cl:
            # Act
            result_csv = await cl.run_query_csv(self.multi_rows_query, filters="number < 3")

        # Assert
        assert pd.read_csv(result_csv.data).to_dict(orient="records") == [
            {"number": 1},
            {"number": 2},
        ]

    @unittest.skip("Large performance tier doesn't currently work.")
    async def test_run_query_context_manager_performance_large(self):
        async with AsyncDuneClient() as cl:
            results = (await cl.run_query(self.query, performance="large")).get_rows()
        assert len(results) > 0

    async def test_get_latest_result_with_query_object(self):
        async with AsyncDuneClient() as cl:
            results = (await cl.get_latest_result(self.query)).get_rows()
        assert len(results) > 0

    async def test_get_latest_result_with_query_id(self):
        async with AsyncDuneClient() as cl:
            results = (await cl.get_latest_result(self.query.query_id)).get_rows()
        assert len(results) > 0


if __name__ == "__main__":
    unittest.main()
