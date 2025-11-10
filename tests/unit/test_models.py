import csv
import json
import unittest
from datetime import datetime
from io import BytesIO, TextIOWrapper

from dateutil.parser import parse
from dateutil.tz import tzutc

from dune_client.models import (
    CreateTableResult,
    DuneError,
    ExecutionError,
    ExecutionResponse,
    ExecutionResult,
    ExecutionResultCSV,
    ExecutionState,
    ExecutionStatusResponse,
    ResultMetadata,
    ResultsResponse,
    TimeData,
    UsageResponse,
)
from dune_client.query import DuneQuery, QueryBase, QueryMeta
from dune_client.types import QueryParameter


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.execution_id = "01GBM4W2N0NMCGPZYW8AYK4YF1"
        self.query_id = 980708
        self.submission_time_str = "2022-08-29T06:33:24.913138Z"
        self.execution_start_str = "2022-08-29T06:33:24.916543331Z"
        self.execution_end_str = "2022-08-29T06:33:25.816543331Z"

        self.execution_response_data = {
            "execution_id": self.execution_id,
            "state": "QUERY_STATE_PENDING",
        }
        self.status_response_data = {
            "execution_id": self.execution_id,
            "query_id": self.query_id,
            "state": "QUERY_STATE_EXECUTING",
            "submitted_at": self.submission_time_str,
            "execution_started_at": self.execution_start_str,
            "execution_ended_at": self.execution_end_str,
        }
        self.result_metadata_data = {
            "column_names": ["ct", "TableName"],
            "column_types": ["x", "y"],
            "row_count": 8,
            "result_set_bytes": 194,
            "total_result_set_bytes": 194,
            "total_row_count": 8,
            "datapoint_count": 2,
            "pending_time_millis": 54,
            "execution_time_millis": 900,
        }
        self.status_response_data_completed = {
            "execution_id": self.execution_id,
            "query_id": self.query_id,
            "state": "QUERY_STATE_COMPLETED",
            "submitted_at": self.submission_time_str,
            "execution_started_at": self.execution_start_str,
            "execution_ended_at": self.execution_end_str,
            "result_metadata": self.result_metadata_data,
        }
        self.results_response_data = {
            "execution_id": self.execution_id,
            "query_id": self.query_id,
            "state": "QUERY_STATE_COMPLETED",
            "submitted_at": self.submission_time_str,
            "expires_at": "2024-08-28T06:36:41.58847Z",
            "execution_started_at": self.execution_start_str,
            "execution_ended_at": self.execution_end_str,
            "result": {
                "rows": [
                    {"TableName": "eth_blocks", "ct": 6296},
                    {"TableName": "eth_traces", "ct": 4474223},
                ],
                "metadata": self.result_metadata_data,
            },
        }
        self.execution_result_csv_data = BytesIO(
            b"""TableName,ct
eth_blocks,6296
eth_traces,4474223
""",
        )

    def test_execution_response_parsing(self):
        expected = ExecutionResponse(
            execution_id="01GBM4W2N0NMCGPZYW8AYK4YF1",
            state=ExecutionState.PENDING,
        )

        assert expected == ExecutionResponse.from_dict(self.execution_response_data)

    def test_parse_time_data(self):
        expected_with_end = TimeData(
            submitted_at=parse(self.submission_time_str),
            expires_at=datetime(2024, 8, 28, 6, 36, 41, 588470, tzinfo=tzutc()),
            execution_started_at=parse(self.execution_start_str),
            execution_ended_at=parse(self.execution_end_str),
            cancelled_at=None,
        )
        assert expected_with_end == TimeData.from_dict(self.results_response_data)

        expected_with_empty_optionals = TimeData(
            submitted_at=parse(self.submission_time_str),
            expires_at=None,
            execution_started_at=parse(self.execution_start_str),
            execution_ended_at=parse(self.execution_end_str),
            cancelled_at=None,
        )
        assert expected_with_empty_optionals == TimeData.from_dict(self.status_response_data)

    def test_parse_status_response(self):
        expected = ExecutionStatusResponse(
            execution_id="01GBM4W2N0NMCGPZYW8AYK4YF1",
            query_id=980708,
            state=ExecutionState.EXECUTING,
            times=TimeData.from_dict(self.status_response_data),
            result_metadata=None,
            queue_position=None,
            error=None,
        )
        assert expected == ExecutionStatusResponse.from_dict(self.status_response_data)

    def test_parse_known_status_response(self):
        # For context: https://github.com/cowprotocol/dune-client/issues/22
        response = {
            "execution_id": "01GES18035K5C4GDTY12Q79GBD",
            "query_id": 1317323,
            "state": "QUERY_STATE_COMPLETED",
            "submitted_at": "2022-10-07T10:53:18.822127Z",
            "expires_at": "2024-10-06T10:53:20.729373Z",
            "execution_started_at": "2022-10-07T10:53:18.823105936Z",
            "execution_ended_at": "2022-10-07T10:53:20.729372559Z",
            "result_metadata": {
                "column_names": ["token"],
                "column_types": ["varchar"],
                "result_set_bytes": 815,
                "total_row_count": 18,
                "datapoint_count": 18,
                "execution_time_millis": 1906,
            },
        }
        try:
            ExecutionStatusResponse.from_dict(response)
        except DuneError as err:
            self.fail(f"Unexpected error {err}")

    def test_parse_status_response_completed(self):
        assert ExecutionStatusResponse(
            execution_id="01GBM4W2N0NMCGPZYW8AYK4YF1",
            query_id=980708,
            state=ExecutionState.COMPLETED,
            times=TimeData.from_dict(self.status_response_data),
            result_metadata=ResultMetadata.from_dict(self.result_metadata_data),
            queue_position=None,
            error=None,
        ) == ExecutionStatusResponse.from_dict(self.status_response_data_completed)

    def test_parse_status_response_with_error(self):
        """Test that errors are properly included in status response (new API feature)"""
        status_response_with_error = {
            "execution_id": "01GBM4W2N0NMCGPZYW8AYK4YF1",
            "query_id": 980708,
            "state": "QUERY_STATE_FAILED",
            "submitted_at": self.submission_time_str,
            "execution_started_at": self.execution_start_str,
            "execution_ended_at": self.execution_end_str,
            "error": {
                "type": "FAILED_TYPE_EXECUTION_FAILED",
                "message": "line 24:13: Binary literal can only contain hexadecimal digits",
                "metadata": {"line": 24, "column": 13},
            },
        }
        result = ExecutionStatusResponse.from_dict(status_response_with_error)
        assert result.state == ExecutionState.FAILED
        assert result.error is not None
        assert isinstance(result.error, ExecutionError)
        assert result.error.type == "FAILED_TYPE_EXECUTION_FAILED"
        assert (
            result.error.message == "line 24:13: Binary literal can only contain hexadecimal digits"
        )
        assert result.error.metadata == {"line": 24, "column": 13}

    def test_parse_result_metadata(self):
        expected = ResultMetadata(
            column_names=["ct", "TableName"],
            column_types=["x", "y"],
            row_count=8,
            result_set_bytes=194,
            total_row_count=8,
            total_result_set_bytes=194,
            datapoint_count=2,
            pending_time_millis=54,
            execution_time_millis=900,
        )
        assert expected == ResultMetadata.from_dict(
            self.results_response_data["result"]["metadata"]
        )
        assert expected == ResultMetadata.from_dict(
            self.status_response_data_completed["result_metadata"]
        )

    def test_parse_execution_result(self):
        expected = ExecutionResult(
            rows=[
                {"TableName": "eth_blocks", "ct": 6296},
                {"TableName": "eth_traces", "ct": 4474223},
            ],
            # Parsing tested above in test_result_metadata_parsing
            metadata=ResultMetadata.from_dict(self.results_response_data["result"]["metadata"]),
        )

        assert expected == ExecutionResult.from_dict(self.results_response_data["result"])

    def test_parse_result_response(self):
        # Time data parsing tested above in test_time_data_parsing.
        time_data = TimeData.from_dict(self.results_response_data)
        expected = ResultsResponse(
            execution_id=self.execution_id,
            query_id=self.query_id,
            state=ExecutionState.COMPLETED,
            times=time_data,
            # Execution result parsing tested above in test_execution_result
            result=ExecutionResult.from_dict(self.results_response_data["result"]),
            next_uri=None,
            next_offset=None,
        )
        assert expected == ResultsResponse.from_dict(self.results_response_data)

    def test_execution_result_csv(self):
        # document the expected output data from DuneAPI result/csv endpoint
        csv_response = ExecutionResultCSV(data=self.execution_result_csv_data)
        result = csv.reader(TextIOWrapper(csv_response.data))
        # note that CSV is non-typed, up to the reader to do type inference
        assert list(result) == [
            ["TableName", "ct"],
            ["eth_blocks", "6296"],
            ["eth_traces", "4474223"],
        ]

    def test_dune_query_from_dict(self):
        example_response = """{
            "query_id": 60066,
            "name": "Ethereum transactions",
            "description": "Returns ethereum transactions starting from the oldest by block time",
            "tags": ["ethereum", "transactions"],
            "version": 15,
            "parameters": [{"key": "limit", "value": "5", "type": "number"}],
            "query_engine": "v2 Dune SQL",
            "query_sql": "select block_number from ethereum.transactions limit {{limit}};",
            "is_private": true,
            "is_archived": false,
            "is_unsaved": false,
            "owner": "Owner Name"
        }"""
        expected = DuneQuery(
            base=QueryBase(
                query_id=60066,
                name="Ethereum transactions",
                params=[QueryParameter.from_dict({"key": "limit", "value": "5", "type": "number"})],
            ),
            meta=QueryMeta(
                description="Returns ethereum transactions starting from the oldest by block time",
                tags=["ethereum", "transactions"],
                version=15,
                engine="v2 Dune SQL",
                is_private=True,
                is_archived=False,
                is_unsaved=False,
                owner="Owner Name",
            ),
            sql="select block_number from ethereum.transactions limit {{limit}};",
        )
        assert expected == DuneQuery.from_dict(json.loads(example_response))

    def test_create_table_result_with_already_existed_field(self):
        """Test CreateTableResult parsing when API response includes already_existed field"""
        response_data = {
            "namespace": "test_namespace",
            "table_name": "test_table",
            "full_name": "dune.test_namespace.test_table",
            "example_query": "select * from dune.test_namespace.test_table limit 10",
            "already_existed": False,
            "message": "Table created successfully",
        }
        result = CreateTableResult.from_dict(response_data)
        assert not result.already_existed

    def test_create_table_result_missing_already_existed_field(self):
        """Test CreateTableResult parsing when API response lacks already_existed field (verify default value)"""
        response_data = {
            "namespace": "test_namespace",
            "table_name": "test_table",
            "full_name": "dune.test_namespace.test_table",
            "example_query": "select * from dune.test_namespace.test_table limit 10",
            "message": "Table created successfully",
            # Note: intentionally omitting already_existed field
        }
        result = CreateTableResult.from_dict(response_data)
        # Verify default value is False
        assert not result.already_existed

    def test_usage_response_parsing(self):
        """Test UsageResponse parsing from API response"""
        response_data = {
            "billing_periods": [
                {
                    "credits_included": 100.0,
                    "credits_used": 50.5,
                    "start_date": "2025-01-01",
                    "end_date": "2025-02-01",
                },
                {
                    "credits_included": 100.0,
                    "credits_used": 75.25,
                    "start_date": "2025-02-01",
                    "end_date": "2025-03-01",
                },
            ],
            "bytes_allowed": 1024000,
            "bytes_used": 512000,
            "private_dashboards": 5,
            "private_queries": 10,
        }
        result = UsageResponse.from_dict(response_data)
        assert len(result.billing_periods) == 2
        assert result.billing_periods[0].credits_included == 100.0
        assert result.billing_periods[0].credits_used == 50.5
        assert result.billing_periods[0].start_date == "2025-01-01"
        assert result.billing_periods[1].credits_used == 75.25
        assert result.bytes_allowed == 1024000
        assert result.bytes_used == 512000
        assert result.private_dashboards == 5
        assert result.private_queries == 10

    def test_usage_response_parsing_with_missing_fields(self):
        """Test UsageResponse parsing with missing optional fields defaults to 0"""
        response_data = {}
        result = UsageResponse.from_dict(response_data)
        assert len(result.billing_periods) == 0
        assert result.bytes_allowed == 0
        assert result.bytes_used == 0
        assert result.private_dashboards == 0
        assert result.private_queries == 0


if __name__ == "__main__":
    unittest.main()
