"""
Dataclasses encoding response data from Dune API.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Union

from dateutil.parser import parse
from duneapi.types import DuneRecord


class DuneError(Exception):
    """Possibilities seen so far
    {'error': 'invalid API Key'}
    {'error': 'Query not found'}
    {'error': 'An internal error occured'}
    {'error': 'The requested execution ID (ID: Wonky Job ID) is invalid.'}
    """

    def __init__(self, data: dict[str, str], response_class: str):
        super().__init__(f"Can't build {response_class} from {data}")


class ExecutionState(Enum):
    """
    Enum for possible values of Query Execution
    """

    COMPLETED = "QUERY_STATE_COMPLETED"
    EXECUTING = "QUERY_STATE_EXECUTING"
    PENDING = "QUERY_STATE_PENDING"


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
    expires_at: datetime
    execution_started_at: datetime
    execution_ended_at: Optional[datetime]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TimeData:
        """Constructor from dictionary. See unit test for sample input."""
        end = data.get("execution_ended_at", None)
        return cls(
            submitted_at=parse(data["submitted_at"]),
            expires_at=parse(data["expires_at"]),
            execution_started_at=parse(data["execution_started_at"]),
            execution_ended_at=None if end is None else parse(end),
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

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> ExecutionStatusResponse:
        """Constructor from dictionary. See unit test for sample input."""
        return cls(
            execution_id=data["execution_id"],
            query_id=int(data["query_id"]),
            state=ExecutionState(data["state"]),
            times=TimeData.from_dict(data),  # Sending the entire data dict
        )


@dataclass
class ResultMetadata:
    """
    Representation of Dune's Result Metadata from [Get] Query Results endpoint
    """

    column_names: list[str]
    result_set_bytes: int
    total_row_count: int

    @classmethod
    def from_dict(cls, data: dict[str, int | list[str]]) -> ResultMetadata:
        """Constructor from dictionary. See unit test for sample input."""
        assert isinstance(data["column_names"], list)
        assert isinstance(data["result_set_bytes"], int)
        assert isinstance(data["total_row_count"], int)
        return cls(
            column_names=data["column_names"],
            result_set_bytes=int(data["result_set_bytes"]),
            total_row_count=int(data["total_row_count"]),
        )


RowData = list[dict[str, str]]
MetaData = dict[str, Union[int, list[str]]]


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


ResultData = dict[str, Union[RowData, MetaData]]


@dataclass
class ResultsResponse:
    """
    Representation of Response from Dune's [Get] Query Results endpoint
    """

    execution_id: str
    query_id: int
    state: ExecutionState
    times: TimeData
    result: ExecutionResult

    @classmethod
    def from_dict(cls, data: dict[str, str | int | ResultData]) -> ResultsResponse:
        """Constructor from dictionary. See unit test for sample input."""
        assert isinstance(data["execution_id"], str)
        assert isinstance(data["query_id"], int)
        assert isinstance(data["state"], str)
        assert isinstance(data["result"], dict)
        return cls(
            execution_id=data["execution_id"],
            query_id=int(data["query_id"]),
            state=ExecutionState(data["state"]),
            times=TimeData.from_dict(data),
            result=ExecutionResult.from_dict(data["result"]),
        )
