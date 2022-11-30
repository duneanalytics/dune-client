import copy
import os
import time
import unittest

import dotenv
from dune_client.types import QueryParameter
from dune_client.client import (
    DuneClient,
    ExecutionResponse,
    ExecutionStatusResponse,
    ExecutionState,
    DuneError,
)
from dune_client.query import Query


class TestDuneClient(unittest.TestCase):
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

    def test_get_status(self):
        query = Query(name="No Name", query_id=1276442, params=[])
        dune = DuneClient(self.valid_api_key)
        job_id = dune.execute(query).execution_id
        status = dune.get_status(job_id)
        self.assertTrue(
            status.state in [ExecutionState.EXECUTING, ExecutionState.PENDING]
        )

    def test_refresh(self):
        dune = DuneClient(self.valid_api_key)
        results = dune.refresh(self.query).get_rows()
        self.assertGreater(len(results), 0)

    def test_parameters_recognized(self):
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

        dune = DuneClient(self.valid_api_key)
        results = dune.refresh(query)
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

    def test_endpoints(self):
        dune = DuneClient(self.valid_api_key)
        execution_response = dune.execute(self.query)
        self.assertIsInstance(execution_response, ExecutionResponse)
        job_id = execution_response.execution_id
        status = dune.get_status(job_id)
        self.assertIsInstance(status, ExecutionStatusResponse)
        while dune.get_status(job_id).state != ExecutionState.COMPLETED:
            time.sleep(1)
        results = dune.get_result(job_id).result.rows
        self.assertGreater(len(results), 0)

    def test_cancel_execution(self):
        dune = DuneClient(self.valid_api_key)
        query = Query(
            name="Long Running Query",
            query_id=1229120,
        )
        execution_response = dune.execute(query)
        job_id = execution_response.execution_id
        # POST Cancellation
        success = dune.cancel_execution(job_id)
        self.assertTrue(success)

        results = dune.get_result(job_id)
        self.assertEqual(results.state, ExecutionState.CANCELLED)

    def test_invalid_api_key_error(self):
        dune = DuneClient(api_key="Invalid Key")
        with self.assertRaises(DuneError) as err:
            dune.execute(self.query)
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionResponse from {'error': 'invalid API Key'}",
        )
        with self.assertRaises(DuneError) as err:
            dune.get_status("wonky job_id")
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionStatusResponse from {'error': 'invalid API Key'}",
        )
        with self.assertRaises(DuneError) as err:
            dune.get_result("wonky job_id")
        self.assertEqual(
            str(err.exception),
            "Can't build ResultsResponse from {'error': 'invalid API Key'}",
        )

    def test_query_not_found_error(self):
        dune = DuneClient(self.valid_api_key)
        query = copy.copy(self.query)
        query.query_id = 99999999  # Invalid Query Id.

        with self.assertRaises(DuneError) as err:
            dune.execute(query)
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionResponse from {'error': 'Query not found'}",
        )

    def test_internal_error(self):
        dune = DuneClient(self.valid_api_key)
        query = copy.copy(self.query)
        # This query ID is too large!
        query.query_id = 9999999999999

        with self.assertRaises(DuneError) as err:
            dune.execute(query)
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionResponse from {'error': 'Query not found'}",
        )

    def test_invalid_job_id_error(self):
        dune = DuneClient(self.valid_api_key)

        with self.assertRaises(DuneError) as err:
            dune.get_status("Wonky Job ID")
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionStatusResponse from "
            "{'error': 'The requested execution ID (ID: Wonky Job ID) is invalid.'}",
        )


if __name__ == "__main__":
    unittest.main()
