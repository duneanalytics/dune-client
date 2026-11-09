import datetime as dt
import unittest

import pytest

from dune_client.query import QueryBase
from dune_client.types import Address, QueryParameter


class TestAddress(unittest.TestCase):
    def setUp(self) -> None:
        self.lower_case_address = "0xde1c59bc25d806ad9ddcbe246c4b5e5505645718"
        self.check_sum_address = "0xDEf1CA1fb7FBcDC777520aa7f396b4E015F497aB"
        self.invalid_address = "0x12"
        self.dune_format = "\\x5d4020b9261f01b6f8a45db929704b0ad6f5e9e6"

    def test_invalid(self):
        with pytest.raises(ValueError) as err:
            Address(address=self.invalid_address)
        assert str(err.value) == f"Invalid Ethereum Address {self.invalid_address}"

    def test_valid(self):
        assert (
            Address(address=self.lower_case_address).address
            == "0xde1c59bc25d806ad9ddcbe246c4b5e5505645718"
        )
        assert (
            Address(address=self.check_sum_address).address
            == "0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab"
        )
        assert (
            Address(address=self.dune_format).address
            == "0x5d4020b9261f01b6f8a45db929704b0ad6f5e9e6"
        )

    def test_inequality(self):
        zero = Address("0x0000000000000000000000000000000000000000")
        one = Address("0x1000000000000000000000000000000000000000")
        a_lower = Address("0xa000000000000000000000000000000000000000")
        a_upper = Address("0xA000000000000000000000000000000000000000")
        b_lower = Address("0xb000000000000000000000000000000000000000")
        c_upper = Address("0xC000000000000000000000000000000000000000")
        assert zero < one
        assert one < a_lower
        assert a_lower == a_upper
        assert a_upper < b_lower
        assert b_lower < c_upper


class TestQueryParameter(unittest.TestCase):
    def setUp(self) -> None:
        self.number_type = QueryParameter.number_type("Number", 1)
        self.text_type = QueryParameter.text_type("Text", "hello")
        self.date_type = QueryParameter.date_type("Date", dt.datetime(2022, 3, 10))

    def test_constructors_and_to_dict(self):
        assert self.number_type.to_dict() == {"key": "Number", "type": "number", "value": "1"}
        assert self.text_type.to_dict() == {"key": "Text", "type": "text", "value": "hello"}
        assert self.date_type.to_dict() == {
            "key": "Date",
            "type": "datetime",
            "value": "2022-03-10 00:00:00",
        }

    def test_repr_method(self):
        query = QueryBase(
            query_id=1,
            name="Test Query",
            params=[self.number_type, self.text_type],
        )

        assert (
            str(query) == "QueryBase(query_id=1, name='Test Query', "
            "params=["
            "Parameter(name=Number, value=1, type=number), "
            "Parameter(name=Text, value=hello, type=text)"
            "])"
        )


@pytest.mark.parametrize("value", [0, -12, 10**40 + 1, 1.25, 1e-6, -1e-7, 1e20])
def test_number_parameter_roundtrip(value):
    parameter = QueryParameter.number_type("minimum_usd", value)
    decoded = QueryParameter.from_dict(parameter.to_dict())
    assert decoded == parameter
    assert type(decoded.value) is type(value)


def test_number_parameter_uppercase_exponent():
    parameter = QueryParameter.from_dict({"key": "minimum_usd", "type": "number", "value": "1E-6"})
    assert parameter == QueryParameter.number_type("minimum_usd", 1e-6)


@pytest.mark.parametrize("value", ["", "not a number", "NaN", "Infinity", "1e", "1e+", "1e2e3"])
def test_invalid_number_parameter(value):
    with pytest.raises(ValueError):
        QueryParameter.from_dict({"key": "minimum_usd", "type": "number", "value": value})


if __name__ == "__main__":
    unittest.main()
