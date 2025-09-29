import unittest

import aiounittest
import pandas as pd

from dune_client.client_async import AsyncDuneClient
from dune_client.models import ExecutionState
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
        results = (await dune.refresh(self.query)).get_rows()
        assert len(results) > 0
        await dune.disconnect()
        assert dune._session.closed

    async def test_refresh_context_manager_singleton(self):
        dune = AsyncDuneClient()
        async with dune as cl:
            results = (await cl.refresh(self.query)).get_rows()
        assert len(results) > 0

    async def test_refresh_context_manager(self):
        async with AsyncDuneClient() as cl:
            results = (await cl.refresh(self.query)).get_rows()
        assert len(results) > 0

    async def test_refresh_with_pagination(self):
        # Arrange
        async with AsyncDuneClient() as cl:
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
        async with AsyncDuneClient() as cl:
            # Act
            results = (await cl.refresh(self.multi_rows_query, filters="number < 3")).get_rows()

        # Assert
        assert results == [{"number": 1}, {"number": 2}]

    async def test_refresh_csv_with_pagination(self):
        # Arrange
        async with AsyncDuneClient() as cl:
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
        async with AsyncDuneClient() as cl:
            # Act
            result_csv = await cl.refresh_csv(self.multi_rows_query, filters="number < 3")

        # Assert
        assert pd.read_csv(result_csv.data).to_dict(orient="records") == [
            {"number": 1},
            {"number": 2},
        ]

    async def test_refresh_context_manager_performance_large(self):
        async with AsyncDuneClient() as cl:
            results = (await cl.refresh(self.query, performance="large")).get_rows()
        assert len(results) > 0

    async def test_get_latest_result_with_query_object(self):
        async with AsyncDuneClient() as cl:
            results = (await cl.get_latest_result(self.query)).get_rows()
        assert len(results) > 0

    async def test_get_latest_result_with_query_id(self):
        async with AsyncDuneClient() as cl:
            results = (await cl.get_latest_result(self.query.query_id)).get_rows()
        assert len(results) > 0

    async def test_execute(self):
        async with AsyncDuneClient() as cl:
            execution_response = await cl.execute(self.query)
            assert execution_response.execution_id is not None
            assert execution_response.state in [
                ExecutionState.EXECUTING,
                ExecutionState.PENDING,
                ExecutionState.COMPLETED,
            ]

    async def test_get_status(self):
        async with AsyncDuneClient() as cl:
            # First execute a query to get a job ID
            execution_response = await cl.execute(self.query)
            job_id = execution_response.execution_id

            # Then get its status
            status = await cl.get_status(job_id)
            assert status.execution_id == job_id
            assert status.state is not None

    async def test_get_result(self):
        async with AsyncDuneClient() as cl:
            # Execute and wait for completion using refresh
            results = await cl.refresh(self.query)
            job_id = results.execution_id

            # Now get results directly
            direct_results = await cl.get_result(job_id)
            assert direct_results.execution_id == job_id
            assert len(direct_results.get_rows()) > 0

    async def test_get_result_csv(self):
        async with AsyncDuneClient() as cl:
            # Execute and wait for completion using refresh
            results = await cl.refresh(self.query)
            job_id = results.execution_id

            # Now get CSV results directly
            csv_results = await cl.get_result_csv(job_id)
            assert csv_results.data is not None
            # Verify we can read the CSV
            import pandas as pd

            df = pd.read_csv(csv_results.data)
            assert len(df) > 0

    async def test_cancel_execution(self):
        async with AsyncDuneClient() as cl:
            # Execute a query
            execution_response = await cl.execute(self.query)
            job_id = execution_response.execution_id

            # Try to cancel it (might already be completed, but that's ok)
            success = await cl.cancel_execution(job_id)
            # success can be True or False depending on timing
            assert isinstance(success, bool)


if __name__ == "__main__":
    unittest.main()
