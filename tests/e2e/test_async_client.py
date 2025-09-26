import os
import unittest

import aiounittest
import dotenv
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
        dotenv.load_dotenv()
        self.valid_api_key = os.environ["DUNE_API_KEY"]

    async def test_disconnect(self):
        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()
        results = (await dune.refresh(self.query)).get_rows()
        assert len(results) > 0
        await dune.disconnect()
        assert dune._session.closed

    async def test_refresh_context_manager_singleton(self):
        dune = AsyncDuneClient(self.valid_api_key)
        async with dune as cl:
            results = (await cl.refresh(self.query)).get_rows()
        assert len(results) > 0

    async def test_refresh_context_manager(self):
        async with AsyncDuneClient(self.valid_api_key) as cl:
            results = (await cl.refresh(self.query)).get_rows()
        assert len(results) > 0

    async def test_refresh_with_pagination(self):
        # Arrange
        async with AsyncDuneClient(self.valid_api_key) as cl:
            # Act
            results = (await cl.refresh(self.multi_rows_query, batch_size=1)).get_rows()

        # Assert
        assert results == [
            {"number": 1},
            {"number": 2},
            {"number": 3},
            {"number": 4},
            {"number": 5},
        ]

    async def test_refresh_with_filters(self):
        # Arrange
        async with AsyncDuneClient(self.valid_api_key) as cl:
            # Act
            results = (await cl.refresh(self.multi_rows_query, filters="number < 3")).get_rows()

        # Assert
        assert results == [{"number": 1}, {"number": 2}]

    async def test_refresh_csv_with_pagination(self):
        # Arrange
        async with AsyncDuneClient(self.valid_api_key) as cl:
            # Act
            result_csv = await cl.refresh_csv(self.multi_rows_query, batch_size=1)

        # Assert
        assert pd.read_csv(result_csv.data).to_dict(orient="records") == [
            {"number": 1},
            {"number": 2},
            {"number": 3},
            {"number": 4},
            {"number": 5},
        ]

    async def test_refresh_csv_with_filters(self):
        # Arrange
        async with AsyncDuneClient(self.valid_api_key) as cl:
            # Act
            result_csv = await cl.refresh_csv(self.multi_rows_query, filters="number < 3")

        # Assert
        assert pd.read_csv(result_csv.data).to_dict(orient="records") == [
            {"number": 1},
            {"number": 2},
        ]

    @unittest.skip("Large performance tier doesn't currently work.")
    async def test_refresh_context_manager_performance_large(self):
        async with AsyncDuneClient(self.valid_api_key) as cl:
            results = (await cl.refresh(self.query, performance="large")).get_rows()
        assert len(results) > 0

    async def test_get_latest_result_with_query_object(self):
        async with AsyncDuneClient(self.valid_api_key) as cl:
            results = (await cl.get_latest_result(self.query)).get_rows()
        assert len(results) > 0

    async def test_get_latest_result_with_query_id(self):
        async with AsyncDuneClient(self.valid_api_key) as cl:
            results = (await cl.get_latest_result(self.query.query_id)).get_rows()
        assert len(results) > 0


if __name__ == "__main__":
    unittest.main()
