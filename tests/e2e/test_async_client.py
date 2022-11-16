import copy
import os
import time
import unittest

import aiounittest
import dotenv

from dune_client.client_async import AsyncDuneClient
from dune_client.types import QueryParameter
from dune_client.client import (
    ExecutionResponse,
    ExecutionStatusResponse,
    ExecutionState,
    DuneError,
)
from dune_client.query import Query


class TestDuneClient(aiounittest.AsyncTestCase):
    def setUp(self) -> None:
        self.query = Query(
            name="Sample Query",
            query_id=1215383,
            params=[
                # These are the queries default parameters.
                QueryParameter.text_type(name="TextField", value="Plain Text"),
                QueryParameter.number_type(name="NumberField", value=3.1415926535),
                QueryParameter.date_type(name="DateField", value="2022-05-04 00:00:00"),
                QueryParameter.enum_type(name="ListField", value="Option 1"),
            ],
        )
        dotenv.load_dotenv()
        self.valid_api_key = os.environ["DUNE_API_KEY"]

    async def test_get_status(self):
        query = Query(name="No Name", query_id=1276442, params=[])
        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()
        job_id = (await dune.execute(query)).execution_id
        status = await dune.get_status(job_id)
        self.assertTrue(
            status.state in [ExecutionState.EXECUTING, ExecutionState.PENDING]
        )
        await dune.disconnect()

    async def test_refresh(self):
        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()
        results = (await dune.refresh(self.query)).get_rows()
        self.assertGreater(len(results), 0)
        await dune.disconnect()

    async def test_parameters_recognized(self):
        query = copy.copy(self.query)
        new_params = [
            # Using all different values for parameters.
            QueryParameter.text_type(name="TextField", value="different word"),
            QueryParameter.number_type(name="NumberField", value=22),
            QueryParameter.date_type(name="DateField", value="1991-01-01 00:00:00"),
            QueryParameter.enum_type(name="ListField", value="Option 2"),
        ]
        query.params = new_params
        self.assertEqual(query.parameters(), new_params)

        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()
        results = await dune.refresh(query)
        self.assertEqual(
            results.get_rows(),
            [
                {
                    "text_field": "different word",
                    "number_field": "22",
                    "date_field": "1991-01-01 00:00:00",
                    "list_field": "Option 2",
                }
            ],
        )
        await dune.disconnect()

    async def test_endpoints(self):
        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()
        execution_response = await dune.execute(self.query)
        self.assertIsInstance(execution_response, ExecutionResponse)
        job_id = execution_response.execution_id
        status = await dune.get_status(job_id)
        self.assertIsInstance(status, ExecutionStatusResponse)
        state = (await dune.get_status(job_id)).state
        while state != ExecutionState.COMPLETED:
            state = (await dune.get_status(job_id)).state
            time.sleep(1)
        results = (await dune.get_result(job_id)).result.rows
        self.assertGreater(len(results), 0)
        await dune.disconnect()

    async def test_cancel_execution(self):
        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()
        query = Query(
            name="Long Running Query",
            query_id=1229120,
        )
        execution_response = await dune.execute(query)
        job_id = execution_response.execution_id
        # POST Cancellation
        success = await dune.cancel_execution(job_id)
        self.assertTrue(success)

        results = await dune.get_result(job_id)
        self.assertEqual(results.state, ExecutionState.CANCELLED)
        await dune.disconnect()

    async def test_invalid_api_key_error(self):
        dune = AsyncDuneClient(api_key="Invalid Key")
        await dune.connect()
        with self.assertRaises(DuneError) as err:
            await dune.execute(self.query)
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionResponse from {'error': 'invalid API Key'}",
        )
        with self.assertRaises(DuneError) as err:
            await dune.get_status("wonky job_id")
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionStatusResponse from {'error': 'invalid API Key'}",
        )
        with self.assertRaises(DuneError) as err:
            await dune.get_result("wonky job_id")
        self.assertEqual(
            str(err.exception),
            "Can't build ResultsResponse from {'error': 'invalid API Key'}",
        )
        await dune.disconnect()

    async def test_query_not_found_error(self):
        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()
        query = copy.copy(self.query)
        query.query_id = 99999999  # Invalid Query Id.

        with self.assertRaises(DuneError) as err:
            await dune.execute(query)
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionResponse from {'error': 'Query not found'}",
        )
        await dune.disconnect()

    async def test_internal_error(self):
        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()
        query = copy.copy(self.query)
        # This query ID is too large!
        query.query_id = 9999999999999

        with self.assertRaises(DuneError) as err:
            await dune.execute(query)
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionResponse from {'error': 'An internal error occured'}",
        )
        await dune.disconnect()

    async def test_invalid_job_id_error(self):
        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()

        with self.assertRaises(DuneError) as err:
            await dune.get_status("Wonky Job ID")
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionStatusResponse from "
            "{'error': 'The requested execution ID (ID: Wonky Job ID) is invalid.'}",
        )
        await dune.disconnect()

    async def test_disconnect(self):
        dune = AsyncDuneClient(self.valid_api_key)
        await dune.connect()
        results = (await dune.refresh(self.query)).get_rows()
        self.assertGreater(len(results), 0)
        await dune.disconnect()
        self.assertTrue(cl._session.closed)

    async def test_refresh_context_manager_singleton(self):
        dune = AsyncDuneClient(self.valid_api_key)
        async with dune as cl:
            results = (await cl.refresh(self.query)).get_rows()
        self.assertGreater(len(results), 0)

    async def test_refresh_context_manager(self):
        async with AsyncDuneClient(self.valid_api_key) as cl:
            results = (await cl.refresh(self.query)).get_rows()
        self.assertGreater(len(results), 0)


if __name__ == "__main__":
    unittest.main()
