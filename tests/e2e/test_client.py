import copy
import os
import time
import unittest

import dotenv
import pandas

from dune_client.models import (
    ExecutionState,
    ExecutionResponse,
    ExecutionStatusResponse,
    DuneError,
)
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

dotenv.load_dotenv()


class TestDuneClient(unittest.TestCase):
    def setUp(self) -> None:
        self.query = QueryBase(
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
        self.valid_api_key = os.environ["DUNE_API_KEY"]

    def copy_query_and_change_parameters(self) -> QueryBase:
        new_query = copy.copy(self.query)
        new_query.params = [
            # Using all different values for parameters.
            QueryParameter.text_type(name="TextField", value="different word"),
            QueryParameter.number_type(name="NumberField", value=22),
            QueryParameter.date_type(name="DateField", value="1991-01-01 00:00:00"),
            QueryParameter.enum_type(name="ListField", value="Option 2"),
        ]
        self.assertNotEqual(self.query.parameters(), new_query.parameters())
        return new_query

    def test_from_env_constructor(self):
        try:
            DuneClient.from_env()
        except KeyError:
            self.fail("DuneClient.from_env raised unexpectedly!")

    def test_get_execution_status(self):
        query = QueryBase(name="No Name", query_id=1276442, params=[])
        dune = DuneClient(self.valid_api_key)
        job_id = dune.execute_query(query).execution_id
        status = dune.get_execution_status(job_id)
        self.assertTrue(
            status.state in [ExecutionState.EXECUTING, ExecutionState.PENDING]
        )

    def test_run_query(self):
        dune = DuneClient(self.valid_api_key)
        results = dune.run_query(self.query).get_rows()
        self.assertGreater(len(results), 0)

    def test_run_query_performance_large(self):
        dune = DuneClient(self.valid_api_key)
        results = dune.run_query(self.query, performance="large").get_rows()
        self.assertGreater(len(results), 0)

    def test_run_query_dataframe(self):
        dune = DuneClient(self.valid_api_key)
        pd = dune.run_query_dataframe(self.query)
        self.assertGreater(len(pd), 0)

    def test_parameters_recognized(self):
        new_query = self.copy_query_and_change_parameters()
        dune = DuneClient(self.valid_api_key)
        results = dune.run_query(new_query)
        self.assertEqual(
            results.get_rows(),
            [
                {
                    "text_field": "different word",
                    "number_field": 22,
                    "date_field": "1991-01-01 00:00:00.000",
                    "list_field": "Option 2",
                }
            ],
        )

    def test_endpoints(self):
        dune = DuneClient(self.valid_api_key)
        execution_response = dune.execute_query(self.query)
        self.assertIsInstance(execution_response, ExecutionResponse)
        job_id = execution_response.execution_id
        status = dune.get_execution_status(job_id)
        self.assertIsInstance(status, ExecutionStatusResponse)
        while dune.get_execution_status(job_id).state != ExecutionState.COMPLETED:
            time.sleep(1)
        results = dune.get_execution_results(job_id).result.rows
        self.assertGreater(len(results), 0)

    def test_cancel_execution(self):
        dune = DuneClient(self.valid_api_key)
        query = QueryBase(
            name="Long Running Query",
            query_id=1229120,
        )
        execution_response = dune.execute_query(query)
        job_id = execution_response.execution_id
        # POST Cancellation
        success = dune.cancel_execution(job_id)
        self.assertTrue(success)

        results = dune.get_execution_results(job_id)
        self.assertEqual(results.state, ExecutionState.CANCELLED)

    def test_invalid_api_key_error(self):
        dune = DuneClient(api_key="Invalid Key")
        with self.assertRaises(DuneError) as err:
            dune.execute_query(self.query)
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionResponse from {'error': 'invalid API Key'}",
        )
        with self.assertRaises(DuneError) as err:
            dune.get_execution_status("wonky job_id")
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionStatusResponse from {'error': 'invalid API Key'}",
        )
        with self.assertRaises(DuneError) as err:
            dune.get_execution_results("wonky job_id")
        self.assertEqual(
            str(err.exception),
            "Can't build ResultsResponse from {'error': 'invalid API Key'}",
        )

    def test_query_not_found_error(self):
        dune = DuneClient(self.valid_api_key)
        query = copy.copy(self.query)
        query.query_id = 99999999  # Invalid Query Id.

        with self.assertRaises(DuneError) as err:
            dune.execute_query(query)
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
            dune.execute_query(query)
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionResponse from {'error': 'An internal error occured'}",
        )

    def test_invalid_job_id_error(self):
        dune = DuneClient(self.valid_api_key)
        with self.assertRaises(DuneError) as err:
            dune.get_execution_status("Wonky Job ID")
        self.assertEqual(
            str(err.exception),
            "Can't build ExecutionStatusResponse from "
            "{'error': 'The requested execution ID (ID: Wonky Job ID) is invalid.'}",
        )

    def test_get_latest_result_with_query_object(self):
        dune = DuneClient(self.valid_api_key)
        results = dune.get_latest_result(self.query).get_rows()
        self.assertGreater(len(results), 0)

    def test_get_latest_result_with_query_id(self):
        dune = DuneClient(self.valid_api_key)
        results = dune.get_latest_result(self.query.query_id).get_rows()
        self.assertGreater(len(results), 0)

    @unittest.skip("This is a plus subscription endpoint.")
    def test_upload_csv_success(self):
        client = DuneClient(self.valid_api_key)
        self.assertEqual(
            client.upload_csv(
                table_name="e2e-test",
                description="best data",
                data="column1,column2\nvalue1,value2\nvalue3,value4",
            ),
            True,
        )

    def test_download_csv_success_by_id(self):
        client = DuneClient(self.valid_api_key)
        new_query = self.copy_query_and_change_parameters()
        # Run query with new parameters
        client.run_query(new_query)
        # Download CSV by query_id
        result_csv = client.download_csv(self.query.query_id)
        # Expect that the csv returns the latest execution results (i.e. those that were just run)
        self.assertEqual(
            pandas.read_csv(result_csv.data).to_dict(orient="records"),
            [
                {
                    "text_field": "different word",
                    "number_field": 22,
                    "date_field": "1991-01-01 00:00:00.000",
                    "list_field": "Option 2",
                }
            ],
        )

    def test_download_csv_success_with_params(self):
        client = DuneClient(self.valid_api_key)
        # Download CSV with query and given parameters.
        result_csv = client.download_csv(self.query)
        # Expect the result to be relative to values of given parameters.
        #################################################################
        # Note that we could compare results with
        #    ",".join([p.value for p in self.query.parameters()]) + "\n"
        # but there seems to be a discrepancy with the date string values.
        # Specifically 1991-01-01 00:00:00.000
        #           vs 1991-01-01 00:00:00
        #################################################################
        self.assertEqual(
            pandas.read_csv(result_csv.data).to_dict(orient="records"),
            [
                {
                    "date_field": "2022-05-04 00:00:00.000",
                    "list_field": "Option 1",
                    "number_field": 3.1415926535,
                    "text_field": "Plain Text",
                }
            ],
        )


@unittest.skip("This is an enterprise only endpoint that can no longer be tested.")
class TestCRUDOps(unittest.TestCase):
    def setUp(self) -> None:
        self.valid_api_key = os.environ["DUNE_API_KEY"]
        self.client = DuneClient(self.valid_api_key, client_version="alpha/v1")
        self.existing_query_id = 2713571

    @unittest.skip("Works fine, but creates too many queries")
    def test_create(self):
        new_query = self.client.create_query(name="test_create", query_sql="")
        self.assertGreater(new_query.base.query_id, 0)

    def test_get(self):
        q_id = 12345
        query = self.client.get_query(q_id)
        self.assertEqual(query.base.query_id, q_id)

    def test_update(self):
        test_id = self.existing_query_id
        current_sql = self.client.get_query(test_id).sql
        self.client.update_query(query_id=test_id, query_sql="")
        self.assertEqual(self.client.get_query(test_id).sql, "")
        # Reset:
        self.client.update_query(query_id=test_id, query_sql=current_sql)

    def test_make_private_and_public(self):
        q_id = self.existing_query_id
        self.client.make_private(q_id)
        self.assertEqual(self.client.get_query(q_id).meta.is_private, True)
        self.client.make_public(q_id)
        self.assertEqual(self.client.get_query(q_id).meta.is_private, False)

    def test_archive(self):
        self.assertEqual(self.client.archive_query(self.existing_query_id), True)
        self.assertEqual(self.client.unarchive_query(self.existing_query_id), False)

    @unittest.skip("Works fine, but creates too many queries!")
    def test_run_sql(self):
        query_sql = "select 85"
        results = self.client.run_sql(query_sql)
        self.assertEqual(results.get_rows(), [{"_col0": 85}])

        # The default functionality is meant to create a private query and then archive it.
        query = self.client.get_query(results.query_id)
        self.assertTrue(query.meta.is_archived)
        self.assertTrue(query.meta.is_private)


if __name__ == "__main__":
    unittest.main()
