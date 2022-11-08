import unittest
from datetime import datetime

from dune_client.query import Query
from dune_client.types import QueryParameter


class TestQueryMonitor(unittest.TestCase):
    def setUp(self) -> None:
        self.date = datetime(year=1985, month=3, day=10)
        self.query_params = [
            QueryParameter.enum_type("Enum", "option1"),
            QueryParameter.text_type("Text", "plain text"),
            QueryParameter.number_type("Number", 12),
            QueryParameter.date_type("Date", "2021-01-01 12:34:56"),
        ]
        self.query = Query(name="", query_id=0, params=self.query_params)

    def test_base_url(self):
        self.assertEqual(self.query.base_url(), "https://dune.com/queries/0")

    def test_url(self):
        self.assertEqual(
            self.query.url(),
            "https://dune.com/queries/0?Enum=option1&Text=plain+text&Number=12&Date=2021-01-01+12%3A34%3A56",
        )
        self.assertEqual(Query(0, "", []).url(), "https://dune.com/queries/0")

    def test_parameters(self):
        self.assertEqual(self.query.parameters(), self.query_params)

    def test_request_format(self):
        expected_answer = {
            "query_parameters": {
                "Enum": "option1",
                "Text": "plain text",
                "Number": "12",
                "Date": "2021-01-01 12:34:56",
            }
        }
        self.assertEqual(self.query.request_format(), expected_answer)

    def test_hash(self):
        # Same ID, different params
        query1 = Query(query_id=0, params=[QueryParameter.text_type("Text", "word1")])
        query2 = Query(query_id=0, params=[QueryParameter.text_type("Text", "word2")])
        self.assertNotEqual(hash(query1), hash(query2))

        # Different ID, same
        query1 = Query(query_id=0)
        query2 = Query(query_id=1)
        self.assertNotEqual(hash(query1), hash(query2))

        # Different ID different params
        query1 = Query(query_id=0)
        query2 = Query(query_id=1, params=[QueryParameter.number_type("num", 1)])
        self.assertNotEqual(hash(query1), hash(query2))


if __name__ == "__main__":
    unittest.main()
