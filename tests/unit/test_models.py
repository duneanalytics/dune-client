import unittest
from datetime import datetime

from dateutil.tz import tzutc

from dune_client.models import (
    ExecutionResponse,
    ExecutionStatusResponse,
    ExecutionState,
    ResultsResponse,
    TimeData,
    ExecutionResult,
    ResultMetadata,
)


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.execution_id = "01GBM4W2N0NMCGPZYW8AYK4YF1"
        self.query_id = 980708

        self.execution_response_data = {
            "execution_id": self.execution_id,
            "state": "QUERY_STATE_PENDING",
        }
        self.status_response_data = {
            "execution_id": self.execution_id,
            "query_id": self.query_id,
            "state": "QUERY_STATE_EXECUTING",
            "submitted_at": "2022-08-29T06:33:24.913138Z",
            "expires_at": "1970-01-01T00:00:00Z",
            "execution_started_at": "2022-08-29T06:33:24.916543331Z",
        }
        self.results_response_data = {
            "execution_id": self.execution_id,
            "query_id": self.query_id,
            "state": "QUERY_STATE_COMPLETED",
            "submitted_at": "2022-08-29T06:33:24.913138Z",
            "expires_at": "2024-08-28T06:36:41.58847Z",
            "execution_started_at": "2022-08-29T06:33:24.916543Z",
            "execution_ended_at": "2022-08-29T06:36:41.588467Z",
            "result": {
                "rows": [
                    {"TableName": "eth_blocks", "ct": 6296},
                    {"TableName": "eth_traces", "ct": 4474223},
                ],
                "metadata": {
                    "column_names": ["ct", "TableName"],
                    "result_set_bytes": 194,
                    "total_row_count": 8,
                },
            },
        }

    def test_execution_response_parsing(self):
        expected = ExecutionResponse(
            execution_id="01GBM4W2N0NMCGPZYW8AYK4YF1",
            state=ExecutionState.PENDING,
        )

        self.assertEqual(
            expected, ExecutionResponse.from_dict(self.execution_response_data)
        )

    def test_parse_time_data(self):
        expected_with_end = TimeData(
            submitted_at=datetime(2022, 8, 29, 6, 33, 24, 913138, tzinfo=tzutc()),
            expires_at=datetime(2024, 8, 28, 6, 36, 41, 588470, tzinfo=tzutc()),
            execution_started_at=datetime(
                2022, 8, 29, 6, 33, 24, 916543, tzinfo=tzutc()
            ),
            execution_ended_at=datetime(2022, 8, 29, 6, 36, 41, 588467, tzinfo=tzutc()),
        )
        self.assertEqual(
            expected_with_end, TimeData.from_dict(self.results_response_data)
        )

        expected_without_end = TimeData(
            submitted_at=datetime(2022, 8, 29, 6, 33, 24, 913138, tzinfo=tzutc()),
            expires_at=datetime(1970, 1, 1, 0, 0, tzinfo=tzutc()),
            execution_started_at=datetime(
                2022, 8, 29, 6, 33, 24, 916543, tzinfo=tzutc()
            ),
            execution_ended_at=None,
        )
        self.assertEqual(
            expected_without_end, TimeData.from_dict(self.status_response_data)
        )

    def test_parse_status_response(self):
        expected = ExecutionStatusResponse(
            execution_id="01GBM4W2N0NMCGPZYW8AYK4YF1",
            query_id=980708,
            state=ExecutionState.EXECUTING,
            times=TimeData.from_dict(self.status_response_data),
        )
        self.assertEqual(
            expected, ExecutionStatusResponse.from_dict(self.status_response_data)
        )

    def test_parse_result_metadata(self):
        expected = ResultMetadata(
            column_names=["ct", "TableName"],
            result_set_bytes=194,
            total_row_count=8,
        )
        self.assertEqual(
            expected,
            ResultMetadata.from_dict(self.results_response_data["result"]["metadata"]),
        )

    def test_parse_execution_result(self):
        expected = ExecutionResult(
            rows=[
                {"TableName": "eth_blocks", "ct": 6296},
                {"TableName": "eth_traces", "ct": 4474223},
            ],
            # Parsing tested above in test_result_metadata_parsing
            metadata=ResultMetadata.from_dict(
                self.results_response_data["result"]["metadata"]
            ),
        )

        self.assertEqual(
            expected, ExecutionResult.from_dict(self.results_response_data["result"])
        )

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
        )
        self.assertEqual(
            expected, ResultsResponse.from_dict(self.results_response_data)
        )


if __name__ == "__main__":
    unittest.main()
