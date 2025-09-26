"""
These types were primarily copied from:
https://github.com/bh2smith/duneapi/blob/v4.0.0/duneapi/types.py
with small adjustments (removing Options from QueryParameter)
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any

from dune_client.util import postgres_date

if TYPE_CHECKING:
    from datetime import datetime

DuneRecord = dict[str, Any]
QueryParameters = dict[str, str | list[str] | int]


class Address:
    """
    Class representing Ethereum Address as a hexadecimal string of length 42.
    The string must begin with '0x' and the other 40 characters
    are digits 0-9 or letters a-f. Upon creation (from string) addresses
    are validated and stored in their check-summed format.
    """

    def __init__(self, address: str):
        # Dune uses \x instead of 0x (i.e. bytea instead of hex string)
        # This is just a courtesy to query writers,
        # so they don't have to convert all addresses to hex strings manually
        address = address.replace("\\x", "0x")
        if Address._is_valid(address):
            self.address: str = address.lower()
        else:
            raise ValueError(f"Invalid Ethereum Address {address}")

    def __str__(self) -> str:
        return str(self.address)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Address):
            return self.address == other.address
        return False

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Address):
            return str(self).lower() < str(other).lower()
        return False

    def __hash__(self) -> int:
        return self.address.__hash__()

    @classmethod
    def zero(cls) -> Address:
        """Returns Null Ethereum Address"""
        return cls("0x0000000000000000000000000000000000000000")

    @classmethod
    def from_int(cls, num: int) -> Address:
        """
        Construct an address from int.
        Used for testing, so that 123 -> "0x0000000000000000000000000000000000000123"
        """
        return cls(f"0x{str(num).rjust(40, '0')}")

    @staticmethod
    def _is_valid(address: str) -> bool:
        match_result = re.match(pattern=r"^(0x)?[0-9a-f]{40}$", string=address, flags=re.IGNORECASE)
        return match_result is not None


class ParameterType(Enum):
    """
    Enum of the 4 distinct dune parameter types
    """

    TEXT = "text"
    NUMBER = "number"
    DATE = "datetime"
    ENUM = "enum"

    @classmethod
    def from_string(cls, type_str: str) -> ParameterType:
        """
        Attempts to parse Parameter from string.
        returns None is no match
        """
        patterns = {
            r"text": cls.TEXT,
            r"number": cls.NUMBER,
            r"date": cls.DATE,
            r"enum": cls.ENUM,
            r"list": cls.ENUM,
        }
        for pattern, param in patterns.items():
            if re.match(pattern, type_str, re.IGNORECASE):
                return param
        raise ValueError(f"could not parse Network from '{type_str}'")


class QueryParameter:
    """Class whose instances are Dune Compatible Query Parameters"""

    def __init__(
        self,
        name: str,
        parameter_type: ParameterType,
        value: Any,
    ):
        self.key: str = name
        self.type: ParameterType = parameter_type
        self.value = value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, QueryParameter):
            return NotImplemented
        return all(
            [
                self.key == other.key,
                self.value == other.value,
                self.type.value == other.type.value,
            ]
        )

    def __hash__(self) -> int:
        value = (
            tuple(self.value)
            if isinstance(self.value, Sequence) and not isinstance(self.value, str)
            else self.value
        )
        return hash((self.key, value, self.type.value))

    @classmethod
    def text_type(cls, name: str, value: str) -> QueryParameter:
        """Constructs a Query parameter of type text"""
        return cls(name, ParameterType.TEXT, value)

    @classmethod
    def number_type(cls, name: str, value: int | float) -> QueryParameter:
        """Constructs a Query parameter of type number"""
        return cls(name, ParameterType.NUMBER, value)

    @classmethod
    def date_type(cls, name: str, value: datetime | str) -> QueryParameter:
        """
        Constructs a Query parameter of type date.
        For convenience, we allow proper datetime type, or string
        """
        if isinstance(value, str):
            value = postgres_date(value)
        return cls(name, ParameterType.DATE, value)

    @classmethod
    def enum_type(cls, name: str, value: str | Sequence[str]) -> QueryParameter:
        """Constructs a Query parameter of type enum or multi-select"""
        if isinstance(value, str):
            return cls(name, ParameterType.ENUM, value)
        if isinstance(value, Sequence):
            return cls(name, ParameterType.ENUM, tuple(value))
        raise TypeError(f"Unsupported enum value type for parameter '{name}': {type(value)!r}")

    def serialized_value(self) -> str | list[str]:
        """Returns JSON-ready value of parameter"""
        if self.type in (ParameterType.TEXT, ParameterType.NUMBER, ParameterType.ENUM):
            if isinstance(self.value, Sequence) and not isinstance(self.value, str):
                return [str(v) for v in self.value]
            return str(self.value)
        if self.type == ParameterType.DATE:
            # This is the postgres string format of timestamptz
            return str(self.value.strftime("%Y-%m-%d %H:%M:%S"))
        raise TypeError(f"Type {self.type} not recognized!")

    def to_dict(self) -> dict[str, str | list[str]]:
        """Converts QueryParameter into string json format accepted by Dune API"""
        results: dict[str, str | list[str]] = {
            "key": self.key,
            "type": self.type.value,
            "value": self.serialized_value(),
        }
        return results

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> QueryParameter:
        """
        Constructs Query Parameters from json.
        TODO - this could probably be done similar to the __init__ method of MetaData
        """
        name, value = obj["key"], obj["value"]
        p_type = ParameterType.from_string(obj["type"])
        if p_type == ParameterType.DATE:
            return cls.date_type(name, value)
        if p_type == ParameterType.TEXT:
            assert isinstance(value, str)
            return cls.text_type(name, value)
        if p_type == ParameterType.NUMBER:
            if isinstance(value, str):
                value = float(value) if "." in value else int(value)
            return cls.number_type(name, value)
        if p_type == ParameterType.ENUM:
            return cls.enum_type(name, value)
        raise ValueError(f"Could not parse Query parameter from {obj}")

    def __str__(self) -> str:
        # For less cryptic logging.
        return f"Parameter(name={self.key}, value={self.value}, type={self.type.value})"

    def __repr__(self) -> str:
        return str(self)
