import copy
import time
import unittest
import warnings
from pathlib import Path

import pandas as pd
import pytest

from dune_client.client import DuneClient
from dune_client.models import (
    ClearTableResult,
    CreateTableResult,
    DeleteTableResult,
    DuneError,
    ExecutionResponse,
    ExecutionState,
    ExecutionStatusResponse,
    InsertTableResult,
)
from dune_client.query import QueryBase
from dune_client.types import QueryParameter


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
        self.multi_rows_query = QueryBase(
            name="Query that returns multiple rows",
            query_id=3435763,
        )

    def copy_query_and_change_parameters(self) -> QueryBase:
        new_query = copy.copy(self.query)
        new_query.params = [
            # Using all different values for parameters.
            QueryParameter.text_type(name="TextField", value="different word"),
            QueryParameter.number_type(name="NumberField", value=22),
            QueryParameter.date_type(name="DateField", value="1991-01-01 00:00:00"),
            QueryParameter.enum_type(name="ListField", value="Option 2"),
        ]
        assert self.query.parameters() != new_query.parameters()
        return new_query

    def test_from_env_constructor(self):
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                DuneClient.from_env()
                # Verify that a deprecation warning was raised
                assert len(w) == 1
                assert issubclass(w[-1].category, DeprecationWarning)
                assert "deprecated" in str(w[-1].message).lower()
        except KeyError:
            self.fail("DuneClient.from_env raised unexpectedly!")

    def test_default_constructor_reads_env(self):
        """Test that the default constructor automatically reads from environment variables"""
        try:
            # This should work the same as from_env() but without the warning
            DuneClient()
        except KeyError:
            self.fail("DuneClient() without arguments should read from environment variables")

    def test_get_execution_status(self):
        query = QueryBase(name="No Name", query_id=1276442, params=[])
        dune = DuneClient()
        job_id = dune.execute_query(query).execution_id
        status = dune.get_execution_status(job_id)
        assert status.state in [ExecutionState.EXECUTING, ExecutionState.PENDING]

    def test_run_query(self):
        dune = DuneClient()
        results = dune.run_query(self.query).get_rows()
        assert len(results) > 0

    def test_run_query_paginated(self):
        # Arrange
        dune = DuneClient()

        # Act
        results = dune.run_query(self.multi_rows_query, batch_size=1).get_rows()

        # Assert
        assert results == [
            {"number": 1},
            {"number": 2},
            {"number": 3},
            {"number": 4},
            {"number": 5},
        ]

    def test_run_query_with_filters(self):
        # Arrange
        dune = DuneClient()

        # Act
        results = dune.run_query(self.multi_rows_query, filters="number < 3").get_rows()

        # Assert
        assert results == [{"number": 1}, {"number": 2}]

    def test_run_query_performance_large(self):
        dune = DuneClient()
        results = dune.run_query(self.query, performance="large").get_rows()
        assert len(results) > 0

    def test_run_query_dataframe(self):
        dune = DuneClient()
        pd = dune.run_query_dataframe(self.query)
        assert len(pd) > 0

    def test_parameters_recognized(self):
        new_query = self.copy_query_and_change_parameters()
        dune = DuneClient()
        results = dune.run_query(new_query)
        assert results.get_rows() == [
            {
                "text_field": "different word",
                "number_field": 22,
                "date_field": "1991-01-01 00:00:00",
                "list_field": "Option 2",
            }
        ]

    def test_endpoints(self):
        dune = DuneClient()
        execution_response = dune.execute_query(self.query)
        assert isinstance(execution_response, ExecutionResponse)
        job_id = execution_response.execution_id
        status = dune.get_execution_status(job_id)
        assert isinstance(status, ExecutionStatusResponse)
        while dune.get_execution_status(job_id).state != ExecutionState.COMPLETED:
            time.sleep(1)
        results = dune.get_execution_results(job_id).result.rows
        assert len(results) > 0

    def test_cancel_execution(self):
        dune = DuneClient()
        query = QueryBase(
            name="Long Running Query",
            query_id=1229120,
        )
        execution_response = dune.execute_query(query)
        job_id = execution_response.execution_id
        # POST Cancellation
        success = dune.cancel_execution(job_id)
        assert success

        results = dune.get_execution_results(job_id)
        assert results.state == ExecutionState.CANCELLED

    def test_invalid_api_key_error(self):
        dune = DuneClient(api_key="Invalid Key")
        with pytest.raises(DuneError) as err:
            dune.execute_query(self.query)
        assert str(err.value) == "Can't build ExecutionResponse from {'error': 'invalid API Key'}"
        with pytest.raises(DuneError) as err:
            dune.get_execution_status("wonky job_id")
        assert (
            str(err.value)
            == "Can't build ExecutionStatusResponse from {'error': 'invalid API Key'}"
        )
        with pytest.raises(DuneError) as err:
            dune.get_execution_results("wonky job_id")
        assert str(err.value) == "Can't build ResultsResponse from {'error': 'invalid API Key'}"

    def test_query_not_found_error(self):
        dune = DuneClient()
        query = copy.copy(self.query)
        query.query_id = 99999999  # Invalid Query Id.

        with pytest.raises(DuneError) as err:
            dune.execute_query(query)
        assert str(err.value) == "Can't build ExecutionResponse from {'error': 'Query not found'}"

    def test_internal_error(self):
        dune = DuneClient()
        query = copy.copy(self.query)
        # This query ID is too large!
        query.query_id = 9999999999999

        with pytest.raises(DuneError) as err:
            dune.execute_query(query)
        assert (
            str(err.value)
            == "Can't build ExecutionResponse from {'error': 'An internal error occured'}"
        )

    def test_invalid_job_id_error(self):
        dune = DuneClient()
        with pytest.raises(DuneError) as err:
            dune.get_execution_status("Wonky Job ID")
        assert (
            str(err.value) == "Can't build ExecutionStatusResponse from "
            "{'error': 'The requested execution ID (ID: Wonky Job ID) is invalid.'}"
        )

    def test_get_latest_result_with_query_object(self):
        dune = DuneClient()
        results = dune.get_latest_result(self.query).get_rows()
        assert len(results) > 0

    def test_get_latest_result_with_query_id(self):
        dune = DuneClient()
        results = dune.get_latest_result(self.query.query_id).get_rows()
        assert len(results) > 0

    @unittest.skip("Requires custom namespace and table_name input.")
    def test_upload_csv_success(self):
        client = DuneClient()
        assert client.upload_csv(
            table_name="e2e-test",
            description="best data",
            data="column1,column2\nvalue1,value2\nvalue3,value4",
        )

    @unittest.skip("Requires custom namespace and table_name input.")
    def test_create_table_success(self):
        # Make sure the table doesn't already exist.
        # You will need to change the namespace to your own.
        client = DuneClient()

        namespace = "bh2smith"
        table_name = "dataset_e2e_test"
        assert client.create_table(
            namespace=namespace,
            table_name=table_name,
            description="e2e test table",
            schema=[{"name": "date", "type": "timestamp"}, {"name": "dgs10", "type": "double"}],
            is_private=False,
        ) == CreateTableResult.from_dict(
            {
                "namespace": namespace,
                "table_name": table_name,
                "full_name": f"dune.{namespace}.{table_name}",
                "example_query": f"select * from dune.{namespace}.{table_name} limit 10",
                "message": "Table created successfully",
            }
        )

    # @unittest.skip("Requires custom namespace and table_name input.")
    def test_create_table_error(self):
        client = DuneClient("Invalid Key")

        namespace = "test"
        table_name = "table"
        with pytest.raises(DuneError):
            client.create_table(
                namespace=namespace,
                table_name=table_name,
                description="",
                schema=[
                    {"name": "ALL_CAPS", "type": "timestamp"},
                ],
                is_private=False,
            )

    @unittest.skip("Requires custom namespace and table_name input.")
    def test_insert_table_csv_success(self):
        # Make sure the table already exists and csv matches table schema.
        # You will need to change the namespace to your own.
        client = DuneClient()
        namespace = "bh2smith"
        table_name = "dataset_e2e_test"
        client.create_table(
            namespace,
            table_name,
            schema=[
                {"name": "date", "type": "timestamp"},
                {"name": "dgs10", "type": "double"},
            ],
        )
        with Path("./tests/fixtures/sample_table_insert.csv").open("rb") as data:
            assert client.insert_table(
                namespace, table_name, data=data, content_type="text/csv"
            ) == InsertTableResult(rows_written=1, bytes_written=33)

    @unittest.skip("Requires custom namespace and table_name input.")
    def test_clear_data(self):
        client = DuneClient()
        namespace = "bh2smith"
        table_name = "dataset_e2e_test"
        assert client.clear_data(namespace, table_name) == ClearTableResult(
            message="Table dune.bh2smith.dataset_e2e_test successfully cleared"
        )

    @unittest.skip("Requires custom namespace and table_name input.")
    def test_insert_table_json_success(self):
        # Make sure the table already exists and json matches table schema.
        # You will need to change the namespace to your own.
        client = DuneClient()
        with Path("./tests/fixtures/sample_table_insert.json").open("rb") as data:
            assert client.insert_table(
                namespace="test",
                table_name="dataset_e2e_test",
                data=data,
                content_type="application/x-ndjson",
            ) == InsertTableResult(rows_written=1, bytes_written=33)

    @unittest.skip("Requires custom namespace and table_name input.")
    def test_delete_table_success(self):
        # Make sure the table doesn't already exist.
        # You will need to change the namespace to your own.
        client = DuneClient()

        namespace = "test"
        table_name = "dataset_e2e_test"

        assert client.delete_table(
            namespace=namespace, table_name=table_name
        ) == DeleteTableResult.from_dict(
            {"message": f"Table {namespace}.{table_name} successfully deleted"}
        )

    def test_download_csv_with_pagination(self):
        # Arrange
        client = DuneClient()
        client.run_query(self.multi_rows_query)

        # Act
        result_csv = client.download_csv(self.multi_rows_query.query_id, batch_size=1)

        # Assert
        assert pd.read_csv(result_csv.data).to_dict(orient="records") == [
            {"number": 1},
            {"number": 2},
            {"number": 3},
            {"number": 4},
            {"number": 5},
        ]

    def test_download_csv_with_filters(self):
        # Arrange
        client = DuneClient()
        client.run_query(self.multi_rows_query)

        # Act
        result_csv = client.download_csv(
            self.multi_rows_query.query_id,
            filters="number < 3",
        )

        # Assert
        assert pd.read_csv(result_csv.data).to_dict(orient="records") == [
            {"number": 1},
            {"number": 2},
        ]

    def test_download_csv_success_by_id(self):
        client = DuneClient()
        new_query = self.copy_query_and_change_parameters()
        # Run query with new parameters
        client.run_query(new_query)
        # Download CSV by query_id
        result_csv = client.download_csv(self.query.query_id)
        # Expect that the csv returns the latest execution results (i.e. those that were just run)
        assert pd.read_csv(result_csv.data).to_dict(orient="records") == [
            {
                "text_field": "different word",
                "number_field": 22,
                "date_field": "1991-01-01 00:00:00",
                "list_field": "Option 2",
            }
        ]

    def test_download_csv_success_with_params(self):
        client = DuneClient()
        # Download CSV with query and given parameters.
        result_csv = client.download_csv(self.query)
        # Expect the result to be relative to values of given parameters.
        #################################################################
        # Note that we could compare results with the query parameters
        # but there seems to be a discrepancy with the date string values.
        # Specifically 1991-01-01 00:00:00 vs 1991-01-01 00:00:00
        #################################################################
        assert pd.read_csv(result_csv.data).to_dict(orient="records") == [
            {
                "date_field": "2022-05-04 00:00:00",
                "list_field": "Option 1",
                "number_field": 3.1415926535,
                "text_field": "Plain Text",
            }
        ]


@unittest.skip("This is an enterprise only endpoint that can no longer be tested.")
class TestCRUDOps(unittest.TestCase):
    def setUp(self) -> None:
        self.client = DuneClient(client_version="alpha/v1")
        self.existing_query_id = 2713571

    @unittest.skip("Works fine, but creates too many queries")
    def test_create(self):
        new_query = self.client.create_query(name="test_create", query_sql="")
        assert new_query.base.query_id > 0

    def test_get(self):
        q_id = 12345
        query = self.client.get_query(q_id)
        assert query.base.query_id == q_id

    def test_update(self):
        test_id = self.existing_query_id
        current_sql = self.client.get_query(test_id).sql
        self.client.update_query(query_id=test_id, query_sql="")
        assert self.client.get_query(test_id).sql == ""
        # Reset:
        self.client.update_query(query_id=test_id, query_sql=current_sql)

    def test_make_private_and_public(self):
        q_id = self.existing_query_id
        self.client.make_private(q_id)
        assert self.client.get_query(q_id).meta.is_private
        self.client.make_public(q_id)
        assert not self.client.get_query(q_id).meta.is_private

    def test_archive(self):
        assert self.client.archive_query(self.existing_query_id)
        assert not self.client.unarchive_query(self.existing_query_id)

    @unittest.skip("Works fine, but creates too many queries!")
    def test_run_sql(self):
        query_sql = "select 85"
        results = self.client.run_sql(query_sql)
        assert results.get_rows() == [{"_col0": 85}]

        # The default functionality is meant to create a private query and then archive it.
        query = self.client.get_query(results.query_id)
        assert query.meta.is_archived
        assert query.meta.is_private


if __name__ == "__main__":
    unittest.main()
