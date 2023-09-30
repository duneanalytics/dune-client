"""
Dataclasses encoding response data from Dune API.
"""
from __future__ import annotations

import logging.config
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from io import BytesIO
from typing import Optional, Any, Union, List, Dict

from dateutil.parser import parse

from dune_client.types import DuneRecord

log = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO
)


class QueryFailed(Exception):
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
    PENDING = "QUERY_STATE_PENDING"
    CANCELLED = "QUERY_STATE_CANCELLED"
    FAILED = "QUERY_STATE_FAILED"

    @classmethod
    def terminal_states(cls) -> set[ExecutionState]:
        """
        Returns the terminal states (i.e. when a query execution is no longer executing
        """
        return {cls.COMPLETED, cls.CANCELLED, cls.FAILED}

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
        return cls(
            execution_id=data["execution_id"], state=ExecutionState(data["state"])
        )


@dataclass
class TimeData:
    """A collection of all timestamp related values contained within Dune Response"""

    submitted_at: datetime
    execution_started_at: Optional[datetime]
    execution_ended_at: Optional[datetime]
    # Expires only exists when we have result data
    expires_at: Optional[datetime]
    # only exists for cancelled executions
    cancelled_at: Optional[datetime]

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
        "type":"FAILED_TYPE_EXECUTION_FAILED",
        "message":"line 24:13: Binary literal can only contain hexadecimal digits",
        "metadata":{"line":24,"column":13}
    }
    """

    type: str
    message: str
    metadata: str

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> ExecutionError:
        """Constructs an instance from a dict"""
        return cls(
            type=data.get("type", "unknown"),
            message=data.get("message", "unknown"),
            metadata=data.get("metadata", "unknown"),
        )


@dataclass
class ExecutionStatusResponse:
    """
    Representation of Response from Dune's [Get] Execution Status endpoint
    """

    execution_id: str
    query_id: int
    state: ExecutionState
    times: TimeData
    queue_position: Optional[int]
    # this will be present when the query execution completes
    result_metadata: Optional[ResultMetadata]
    error: Optional[ExecutionError]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionStatusResponse:
        """Constructor from dictionary. See unit test for sample input."""
        dct: Optional[MetaData] = data.get("result_metadata")
        error: Optional[dict[str, str]] = data.get("error")
        return cls(
            execution_id=data["execution_id"],
            query_id=int(data["query_id"]),
            queue_position=data.get("queue_position"),
            state=ExecutionState(data["state"]),
            result_metadata=ResultMetadata.from_dict(dct) if dct else None,
            times=TimeData.from_dict(data),  # Sending the entire data dict
            error=ExecutionError.from_dict(error) if error else None,
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
    result_set_bytes: int
    total_row_count: int
    datapoint_count: int
    pending_time_millis: Optional[int]
    execution_time_millis: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResultMetadata:
        """Constructor from dictionary. See unit test for sample input."""
        assert isinstance(data["column_names"], list)
        pending_time = data.get("pending_time_millis", None)
        return cls(
            column_names=data["column_names"],
            result_set_bytes=int(data["result_set_bytes"]),
            total_row_count=int(data["total_row_count"]),
            datapoint_count=int(data["datapoint_count"]),
            pending_time_millis=int(pending_time) if pending_time else None,
            execution_time_millis=int(data["execution_time_millis"]),
        )


RowData = List[Dict[str, str]]
MetaData = Dict[str, Union[int, List[str]]]


@dataclass
class ExecutionResultCSV:
    """
    Representation of a raw `result` in CSV format
    this payload can be passed directly to
        csv.reader(data) or
        pandas.read_csv(data)
    """

    data: BytesIO  # includes all CSV rows, including the header row.


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


ResultData = Dict[str, Union[RowData, MetaData]]


@dataclass
class ResultsResponse:
    """
    Representation of Response from Dune's [Get] Query Results endpoint
    """

    execution_id: str
    query_id: int
    state: ExecutionState
    times: TimeData
    # optional because it will only be present when the query execution completes
    result: Optional[ExecutionResult]

    @classmethod
    def from_dict(cls, data: dict[str, str | int | ResultData]) -> ResultsResponse:
        """Constructor from dictionary. See unit test for sample input."""
        assert isinstance(data["execution_id"], str)
        assert isinstance(data["query_id"], int)
        assert isinstance(data["state"], str)
        result = data.get("result", {})
        assert isinstance(result, dict)
        return cls(
            execution_id=data["execution_id"],
            query_id=int(data["query_id"]),
            state=ExecutionState(data["state"]),
            times=TimeData.from_dict(data),
            result=ExecutionResult.from_dict(result) if result else None,
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
