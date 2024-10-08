import copy
import os
import time
import unittest

import dotenv

from dune_client.client import DuneClient

dotenv.load_dotenv()


@unittest.skip("endpoint no longer exists - {'error': 'Custom endpoint not found'}")
class TestCustomEndpoints(unittest.TestCase):
    def setUp(self) -> None:
        self.valid_api_key = os.environ["DUNE_API_KEY"]

    def test_getting_custom_endpoint_results(self):
        dune = DuneClient(self.valid_api_key)
        results = dune.get_custom_endpoint_result("dune", "new-test")
        self.assertEqual(len(results.get_rows()), 10)


if __name__ == "__main__":
    unittest.main()
