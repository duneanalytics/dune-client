import json
import unittest
import urllib.parse
from datetime import datetime

from dune_client.query import QueryBase, parse_query_object_or_id
from dune_client.types import QueryParameter


class TestQueryBase(unittest.TestCase):
    def setUp(self) -> None:
        self.date = datetime(year=1985, month=3, day=10)
        self.query_params = [
            QueryParameter.enum_type("Enum", "option1"),
            QueryParameter.text_type("Text", "plain text"),
            QueryParameter.number_type("Number", 12),
            QueryParameter.date_type("Date", "2021-01-01 12:34:56"),
            QueryParameter.enum_type("Multi", ["a1", "a2"]),
        ]
        self.query = QueryBase(name="", query_id=0, params=self.query_params)

    def test_base_url(self):
        assert self.query.base_url() == "https://dune.com/queries/0"

    def test_url(self):
        raw_params = (
            'Enum=option1&Text=plain text&Number=12&Date=2021-01-01 12:34:56&Multi=["a1","a2"]'
        )
        expected_url = "?".join(
            [
                "https://dune.com/queries/0",
                urllib.parse.quote_plus(raw_params, safe="=&?"),
            ]
        )
        assert self.query.url() == expected_url
        assert QueryBase(0, "", []).url() == "https://dune.com/queries/0"

    def test_parameters(self):
        assert self.query.parameters() == self.query_params

    def test_url_round_trips_text_values(self):
        for value in ("cats & dogs", "left=right?yes#section", "100% + value", "café / Δ", ""):
            with self.subTest(value=value):
                query = QueryBase(
                    query_id=123,
                    params=[
                        QueryParameter.text_type("Text", value),
                        QueryParameter.number_type("Number", 12),
                    ],
                )
                parsed = urllib.parse.urlsplit(query.url())
                assert parsed.path == "/queries/123"
                assert not parsed.fragment
                assert urllib.parse.parse_qsl(parsed.query, keep_blank_values=True) == [
                    ("Text", value),
                    ("Number", "12"),
                ]

    def test_url_round_trips_parameter_names(self):
        for name in ("a&b", "a=b", "space key", "κλειδί?"):
            with self.subTest(name=name):
                query = QueryBase(query_id=123, params=[QueryParameter.text_type(name, "value")])
                assert urllib.parse.parse_qsl(urllib.parse.urlsplit(query.url()).query) == [
                    (name, "value")
                ]

    def test_url_round_trips_multiselect_values(self):
        values = ["a&b", "c=d", "question?mark", "space + percent%", "Δ"]
        query = QueryBase(query_id=123, params=[QueryParameter.enum_type("Multi", values)])
        parsed = urllib.parse.parse_qsl(urllib.parse.urlsplit(query.url()).query)
        assert len(parsed) == 1
        assert parsed[0][0] == "Multi"
        assert json.loads(parsed[0][1]) == values

    def test_request_format(self):
        expected_answer = {
            "query_parameters": {
                "Enum": "option1",
                "Text": "plain text",
                "Number": "12",
                "Date": "2021-01-01 12:34:56",
                "Multi": ["a1", "a2"],
            }
        }
        assert self.query.request_format() == expected_answer

    def test_hash(self):
        # Same ID, different params
        query1 = QueryBase(query_id=0, params=[QueryParameter.text_type("Text", "word1")])
        query2 = QueryBase(query_id=0, params=[QueryParameter.text_type("Text", "word2")])
        assert hash(query1) != hash(query2)

        # Different ID, same
        query1 = QueryBase(query_id=0)
        query2 = QueryBase(query_id=1)
        assert hash(query1) != hash(query2)

        # Different ID different params
        query1 = QueryBase(query_id=0)
        query2 = QueryBase(query_id=1, params=[QueryParameter.number_type("num", 1)])
        assert hash(query1) != hash(query2)

    def test_parse_object_or_id(self):
        expected_params = {
            "params.Date": "2021-01-01 12:34:56",
            "params.Enum": "option1",
            "params.Number": "12",
            "params.Text": "plain text",
            "params.Multi": ["a1", "a2"],
        }
        expected_query_id = self.query.query_id
        # Query Object
        assert parse_query_object_or_id(self.query) == (expected_params, expected_query_id)
        # Query ID (integer)
        expected_params = None
        assert parse_query_object_or_id(self.query.query_id) == (expected_params, expected_query_id)
        # Query ID (string)
        assert parse_query_object_or_id(str(self.query.query_id)) == (
            expected_params,
            expected_query_id,
        )


if __name__ == "__main__":
    unittest.main()
