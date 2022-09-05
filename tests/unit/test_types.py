import datetime
import unittest

from dune_client.types import QueryParameter, Address


class TestAddress(unittest.TestCase):
    def setUp(self) -> None:
        self.lower_case_address = "0xde1c59bc25d806ad9ddcbe246c4b5e5505645718"
        self.check_sum_address = "0xDEf1CA1fb7FBcDC777520aa7f396b4E015F497aB"
        self.invalid_address = "0x12"
        self.dune_format = "\\x5d4020b9261f01b6f8a45db929704b0ad6f5e9e6"

    def test_invalid(self):
        with self.assertRaises(ValueError) as err:
            Address(address=self.invalid_address)
        self.assertEqual(
            str(err.exception), f"Invalid Ethereum Address {self.invalid_address}"
        )

    def test_valid(self):
        self.assertEqual(
            Address(address=self.lower_case_address).address,
            "0xdE1c59Bc25D806aD9DdCbe246c4B5e5505645718",
        )
        self.assertEqual(
            Address(address=self.check_sum_address).address,
            "0xDEf1CA1fb7FBcDC777520aa7f396b4E015F497aB",
        )
        self.assertEqual(
            Address(address=self.dune_format).address,
            "0x5d4020b9261F01B6f8a45db929704b0Ad6F5e9E6",
        )

    def test_inequality(self):
        zero = Address("0x0000000000000000000000000000000000000000")
        one = Address("0x1000000000000000000000000000000000000000")
        a_lower = Address("0xa000000000000000000000000000000000000000")
        a_upper = Address("0xA000000000000000000000000000000000000000")
        b_lower = Address("0xb000000000000000000000000000000000000000")
        c_upper = Address("0xC000000000000000000000000000000000000000")
        self.assertLess(zero, one)
        self.assertLess(one, a_lower)
        self.assertEqual(a_lower, a_upper)
        self.assertLess(a_upper, b_lower)
        self.assertLess(b_lower, c_upper)


class TestQueryParameter(unittest.TestCase):
    def test_constructors_and_to_dict(self):
        number_type = QueryParameter.number_type("Number", 1)
        text_type = QueryParameter.text_type("Text", "hello")
        date_type = QueryParameter.date_type("Date", datetime.datetime(2022, 3, 10))

        self.assertEqual(
            number_type.to_dict(), {"key": "Number", "type": "number", "value": "1"}
        )
        self.assertEqual(
            text_type.to_dict(), {"key": "Text", "type": "text", "value": "hello"}
        )
        self.assertEqual(
            date_type.to_dict(),
            {"key": "Date", "type": "datetime", "value": "2022-03-10 00:00:00"},
        )


if __name__ == "__main__":
    unittest.main()
