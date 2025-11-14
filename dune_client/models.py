"""
Dataclasses encoding response data from Dune API.
"""

from __future__ import annotations

import logging.config
from dataclasses import dataclass
from enum import Enum
from os import SEEK_END
from typing import TYPE_CHECKING, Any

from dataclasses_json import DataClassJsonMixin
from dateutil.parser import parse

if TYPE_CHECKING:
    from datetime import datetime
    from io import BytesIO

    from dune_client.types import DuneRecord

log = logging.getLogger(__name__)


class QueryFailedError(Exception):
    """Special Error for failed Queries"""


class DuneError(Exception):
    """Possibilities seen so far
    {'error': 'invalid API Key'}
    {'error': 'Query not found'}
    {'error': 'An internal error occured'}
    {'error': 'The requested execution ID (ID: Wonky Job ID) is invalid.'}
    """

    def __init__(self, data: dict[str, str], response_class: str, err: KeyError):
        error_message = f"Can't build {response_class} from {data}"
        log.error(f"{error_message} due to KeyError: {err}")
        super().__init__(error_message)


class ExecutionState(Enum):
    """
    Enum for possible values of Query Execution
    """

    COMPLETED = "QUERY_STATE_COMPLETED"
    EXECUTING = "QUERY_STATE_EXECUTING"
    PARTIAL = "QUERY_STATE_COMPLETED_PARTIAL"
    PENDING = "QUERY_STATE_PENDING"
    CANCELLED = "QUERY_STATE_CANCELLED"
    FAILED = "QUERY_STATE_FAILED"
    EXPIRED = "QUERY_STATE_EXPIRED"

    @classmethod
    def terminal_states(cls) -> set[ExecutionState]:
        """
        Returns the terminal states (i.e. when a query execution is no longer executing
        """
        return {cls.COMPLETED, cls.CANCELLED, cls.FAILED, cls.EXPIRED, cls.PARTIAL}

    def is_complete(self) -> bool:
        """Returns True is state is completed, otherwise False."""
        return self == ExecutionState.COMPLETED


@dataclass
class ExecutionResponse:
    """
    Representation of Response from Dune's [Post] Execute Query ID endpoint
    """

    execution_id: str
    state: ExecutionState

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> ExecutionResponse:
        """Constructor from dictionary. See unit test for sample input."""
        return cls(execution_id=data["execution_id"], state=ExecutionState(data["state"]))


@dataclass
class PipelineExecutionResponse:
    """
    Representation of Response from Dune's [Post] Execute Query Pipeline endpoint
    """

    pipeline_execution_id: str

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> PipelineExecutionResponse:
        """Constructor from dictionary."""
        return cls(pipeline_execution_id=data["pipeline_execution_id"])


@dataclass
class TimeData:
    """A collection of all timestamp related values contained within Dune Response"""

    submitted_at: datetime
    execution_started_at: datetime | None
    execution_ended_at: datetime | None
    # Expires only exists when we have result data
    expires_at: datetime | None
    # only exists for cancelled executions
    cancelled_at: datetime | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TimeData:
        """Constructor from dictionary. See unit test for sample input."""
        start = data.get("execution_started_at")
        end = data.get("execution_ended_at")
        expires = data.get("expires_at")
        cancelled = data.get("cancelled_at")
        return cls(
            submitted_at=parse(data["submitted_at"]),
            expires_at=None if expires is None else parse(expires),
            execution_started_at=None if start is None else parse(start),
            execution_ended_at=None if end is None else parse(end),
            cancelled_at=None if cancelled is None else parse(cancelled),
        )


@dataclass
class ExecutionError:
    """
    Representation of Execution Error Response:

    Example:
    {
        "type":"syntax_error",
        "message":"Error: Line 1:1: mismatched input 'selecdt'",
        "metadata":{"line":10,"column":73}
    }
    """

    type: str
    message: str
    metadata: dict[str, Any] | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionError:
        """Constructs an instance from a dict"""
        return cls(
            type=data.get("type", "unknown"),
            message=data.get("message", "unknown"),
            metadata=data.get("metadata"),
        )


@dataclass
class ExecutionStatusResponse:
    """
    Representation of Response from Dune's [Get] Execution Status endpoint
    https://docs.dune.com/api-reference/executions/endpoint/get-execution-status
    """

    execution_id: str
    query_id: int | None  # None for ad-hoc SQL executions via /sql/execute
    state: ExecutionState
    times: TimeData
    queue_position: int | None
    # this will be present when the query execution completes
    result_metadata: ResultMetadata | None
    error: ExecutionError | None
    # New fields added to the API
    is_execution_finished: bool | None = None
    execution_cost_credits: float | None = None
    max_inflight_interactive_executions: int | None = None
    max_inflight_interactive_reached: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionStatusResponse:
        """Constructor from dictionary. See unit test for sample input."""
        dct: MetaData | None = data.get("result_metadata")
        error: dict[str, Any] | None = data.get("error")
        query_id = data.get("query_id")
        return cls(
            execution_id=data["execution_id"],
            query_id=int(query_id) if query_id is not None else None,
            queue_position=data.get("queue_position"),
            state=ExecutionState(data["state"]),
            result_metadata=ResultMetadata.from_dict(dct) if dct else None,
            times=TimeData.from_dict(data),  # Sending the entire data dict
            error=ExecutionError.from_dict(error) if error else None,
            is_execution_finished=data.get("is_execution_finished"),
            execution_cost_credits=data.get("execution_cost_credits"),
            max_inflight_interactive_executions=data.get("max_inflight_interactive_executions"),
            max_inflight_interactive_reached=data.get("max_inflight_interactive_reached"),
        )

    def __str__(self) -> str:
        if self.state == ExecutionState.PENDING:
            return f"{self.state} (queue position: {self.queue_position})"
        if self.state == ExecutionState.FAILED:
            return (
                f"{self.state}: execution_id={self.execution_id}, "
                f"query_id={self.query_id}, times={self.times}"
            )

        return f"{self.state}"


@dataclass
class ResultMetadata:
    """
    Representation of Dune's Result Metadata from [Get] Query Results endpoint
    """

    column_names: list[str]
    column_types: list[str]
    row_count: int
    result_set_bytes: int
    total_row_count: int
    total_result_set_bytes: int
    datapoint_count: int
    pending_time_millis: int | None
    execution_time_millis: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResultMetadata:
        """Constructor from dictionary. See unit test for sample input."""
        assert isinstance(data["column_names"], list)
        pending_time = data.get("pending_time_millis")
        return cls(
            column_names=data["column_names"],
            column_types=data["column_types"],
            row_count=int(data["total_row_count"]),
            result_set_bytes=int(data["result_set_bytes"]),
            total_row_count=int(data["total_row_count"]),
            total_result_set_bytes=int(data["result_set_bytes"]),
            datapoint_count=int(data["datapoint_count"]),
            pending_time_millis=int(pending_time) if pending_time else None,
            execution_time_millis=int(data["execution_time_millis"]),
        )

    def __add__(self, other: ResultMetadata) -> ResultMetadata:
        """
        Enables combining results by updating the metadata associated to
        an execution by using the `+` operator.
        """
        assert other is not None

        self.row_count += other.row_count
        self.result_set_bytes += other.result_set_bytes
        self.datapoint_count += other.datapoint_count
        return self


RowData = list[dict[str, Any]]
MetaData = dict[str, int | list[str]]


@dataclass
class ExecutionResultCSV:
    """
    Representation of a raw `result` in CSV format
    this payload can be passed directly to
        csv.reader(data) or
        pandas.read_csv(data)
    """

    data: BytesIO  # includes all CSV rows, including the header row.
    next_uri: str | None = None
    next_offset: int | None = None

    def __add__(self, other: ExecutionResultCSV) -> ExecutionResultCSV:
        assert other is not None
        assert other.data is not None

        self.next_uri = other.next_uri
        self.next_offset = other.next_offset

        # Get to the end of the current CSV
        self.data.seek(0, SEEK_END)

        # Skip the first line of the new CSV, which contains the header
        other.data.readline()

        # Append the rest of the content from `other` into current one
        self.data.write(other.data.read())

        # Move the cursor back to the start of the CSV
        self.data.seek(0)

        return self


@dataclass
class ExecutionResult:
    """Representation of `result` field of a Dune ResultsResponse"""

    rows: list[DuneRecord]
    metadata: ResultMetadata

    @classmethod
    def from_dict(cls, data: dict[str, RowData | MetaData]) -> ExecutionResult:
        """Constructor from dictionary. See unit test for sample input."""
        assert isinstance(data["rows"], list)
        assert isinstance(data["metadata"], dict)
        return cls(
            rows=data["rows"],
            metadata=ResultMetadata.from_dict(data["metadata"]),
        )

    def __add__(self, other: ExecutionResult) -> ExecutionResult:
        """
        Enables combining results using the `+` operator.
        """
        self.rows.extend(other.rows)
        self.metadata += other.metadata

        return self


ResultData = dict[str, RowData | MetaData]


@dataclass
class ResultsResponse:
    """
    Representation of Response from Dune's [Get] Query Results endpoint
    """

    execution_id: str
    query_id: int | None  # None for ad-hoc SQL executions via /sql/execute
    state: ExecutionState
    times: TimeData
    # optional because it will only be present when the query execution completes
    result: ExecutionResult | None
    next_uri: str | None
    next_offset: int | None

    @classmethod
    def from_dict(cls, data: dict[str, str | int | ResultData]) -> ResultsResponse:
        """Constructor from dictionary. See unit test for sample input."""
        assert isinstance(data["execution_id"], str)
        query_id = data.get("query_id")
        assert isinstance(query_id, int) or query_id is None
        assert isinstance(data["state"], str)
        result = data.get("result", {})
        assert isinstance(result, dict)
        next_uri = data.get("next_uri")
        assert isinstance(next_uri, str) or next_uri is None
        next_offset = data.get("next_offset")
        assert isinstance(next_offset, int) or next_offset is None
        return cls(
            execution_id=data["execution_id"],
            query_id=int(query_id) if query_id is not None else None,
            state=ExecutionState(data["state"]),
            times=TimeData.from_dict(data),
            result=ExecutionResult.from_dict(result) if result else None,
            next_uri=next_uri,
            next_offset=next_offset,
        )

    def get_rows(self) -> list[DuneRecord]:
        """
        Absorbs the Optional check and returns the result rows.
        When execution is a non-complete terminal state, returns empty list.
        """

        if self.state == ExecutionState.COMPLETED:
            assert self.result is not None, f"No Results on completed execution {self}"
            return self.result.rows

        log.info(f"execution {self.state} returning empty list")
        return []

    def __add__(self, other: ResultsResponse) -> ResultsResponse:
        """
        Enables combining results using the `+` operator.
        """
        assert self.execution_id == other.execution_id
        assert self.result is not None
        assert other.result is not None
        self.result += other.result
        self.next_uri = other.next_uri
        self.next_offset = other.next_offset
        return self


@dataclass
class CreateTableResult(DataClassJsonMixin):
    """
    Data type returned by table/create operation
    """

    namespace: str
    table_name: str
    full_name: str
    example_query: str
    message: str
    # kept for backward compatibility, always False, unreliable
    already_existed: bool | None = False


@dataclass
class InsertTableResult(DataClassJsonMixin):
    """
    Data type returned by table/insert operation
    """

    rows_written: int
    bytes_written: int


@dataclass
class DeleteTableResult(DataClassJsonMixin):
    """
    Data type returned by table/delete operation
    """

    message: str


@dataclass
class ClearTableResult(DataClassJsonMixin):
    """
    Data type returned by table/clear operation
    """

    message: str


@dataclass
class BillingPeriod:
    """Billing period information with credits and dates"""

    credits_included: float
    credits_used: float
    start_date: str
    end_date: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BillingPeriod:
        """Constructor from dictionary."""
        return cls(
            credits_included=float(data["credits_included"]),
            credits_used=float(data["credits_used"]),
            start_date=data["start_date"],
            end_date=data["end_date"],
        )


@dataclass
class UsageResponse:
    """
    Representation of Response from Dune's [POST] Usage endpoint
    https://docs.dune.com/api-reference/usage/endpoint/get-usage
    """

    billing_periods: list[BillingPeriod]
    bytes_allowed: int
    bytes_used: int
    private_dashboards: int
    private_queries: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> UsageResponse:
        """Constructor from dictionary."""
        billing_periods_data = data.get("billing_periods", [])
        return cls(
            billing_periods=[BillingPeriod.from_dict(bp) for bp in billing_periods_data],
            bytes_allowed=int(data.get("bytes_allowed", 0)),
            bytes_used=int(data.get("bytes_used", 0)),
            private_dashboards=int(data.get("private_dashboards", 0)),
            private_queries=int(data.get("private_queries", 0)),
        )


@dataclass
class PipelineQueryExecutionStatus:
    """Query execution status within a pipeline node"""

    status: str
    query_id: int
    execution_id: str | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PipelineQueryExecutionStatus:
        """Constructor from dictionary."""
        return cls(
            status=data["status"],
            query_id=int(data["query_id"]),
            execution_id=data.get("execution_id"),
        )


@dataclass
class PipelineNodeExecution:
    """Pipeline node execution information"""

    id: int
    query_execution_status: PipelineQueryExecutionStatus

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PipelineNodeExecution:
        """Constructor from dictionary."""
        return cls(
            id=int(data["id"]),
            query_execution_status=PipelineQueryExecutionStatus.from_dict(
                data["query_execution_status"]
            ),
        )


@dataclass
class PipelineStatusResponse:
    """
    Representation of Response from Dune's [Get] Pipeline Status endpoint
    """

    status: str
    node_executions: list[PipelineNodeExecution]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PipelineStatusResponse:
        """Constructor from dictionary."""
        return cls(
            status=data["status"],
            node_executions=[PipelineNodeExecution.from_dict(ne) for ne in data["node_executions"]],
        )


class DatasetType(Enum):
    """
    Enum for possible dataset types
    """

    TRANSFORMATION_VIEW = "transformation_view"
    TRANSFORMATION_TABLE = "transformation_table"
    UPLOADED_TABLE = "uploaded_table"
    DECODED_TABLE = "decoded_table"
    SPELL = "spell"
    DUNE_TABLE = "dune_table"


@dataclass
class DatasetOwner(DataClassJsonMixin):
    """Owner information for a dataset"""

    handle: str
    type: str


@dataclass
class DatasetColumn(DataClassJsonMixin):
    """Column information for a dataset"""

    name: str
    type: str
    nullable: bool


@dataclass
class Dataset(DataClassJsonMixin):
    """Dataset information returned by list datasets endpoint"""

    full_name: str
    type: str
    owner: DatasetOwner
    columns: list[DatasetColumn]
    metadata: dict[str, str]
    created_at: str
    updated_at: str
    is_private: bool


@dataclass
class DatasetListResponse(DataClassJsonMixin):
    """Response from GET /v1/datasets"""

    datasets: list[Dataset]
    total: int


@dataclass
class DatasetResponse(DataClassJsonMixin):
    """Response from GET /v1/datasets/{full_name}"""

    full_name: str
    type: str
    owner: DatasetOwner
    columns: list[DatasetColumn]
    metadata: dict[str, str]
    created_at: str
    updated_at: str
    is_private: bool


@dataclass
class TableOwner(DataClassJsonMixin):
    """Owner information for an uploaded table"""

    handle: str
    type: str


@dataclass
class TableColumn(DataClassJsonMixin):
    """Column information for an uploaded table"""

    name: str
    type: str
    nullable: bool


@dataclass
class TableElement(DataClassJsonMixin):
    """Individual table metadata in list response"""

    full_name: str
    is_private: bool
    table_size_bytes: str
    created_at: str
    updated_at: str
    owner: TableOwner
    columns: list[TableColumn]


@dataclass
class UploadListResponse(DataClassJsonMixin):
    """Response from GET /v1/uploads"""

    tables: list[TableElement]
    next_offset: int | None = None


@dataclass
class UploadCreateRequest:
    """Request for POST /v1/uploads"""

    namespace: str
    table_name: str
    schema: list[dict[str, str]]
    description: str = ""
    is_private: bool = False


@dataclass
class UploadCreateResponse(DataClassJsonMixin):
    """Response from POST /v1/uploads"""

    namespace: str
    table_name: str
    full_name: str
    example_query: str


@dataclass
class CSVUploadRequest:
    """Request for POST /v1/uploads/csv"""

    table_name: str
    data: str
    description: str = ""
    is_private: bool = False


@dataclass
class CSVUploadResponse(DataClassJsonMixin):
    """Response from POST /v1/uploads/csv"""

    table_name: str


@dataclass
class InsertDataResponse(DataClassJsonMixin):
    """Response from POST /v1/uploads/{namespace}/{table_name}/insert"""

    rows_written: int
    bytes_written: int
    name: str


@dataclass
class ClearTableResponse(DataClassJsonMixin):
    """Response from POST /v1/uploads/{namespace}/{table_name}/clear"""

    message: str


@dataclass
class DeleteTableResponse(DataClassJsonMixin):
    """Response from DELETE /v1/uploads/{namespace}/{table_name}"""

    message: str
