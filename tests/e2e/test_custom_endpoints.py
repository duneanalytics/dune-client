import unittest
import warnings

from dune_client.client import DuneClient


@unittest.skip("endpoint no longer exists - {'error': 'Custom endpoint not found'}")
class TestCustomEndpoints(unittest.TestCase):
    def test_getting_custom_endpoint_results(self):
        dune = DuneClient()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            results = dune.get_custom_endpoint_result("dune", "new-test")
            assert len(results.get_rows()) == 10
            # Verify that a deprecation warning was issued
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message).lower()


if __name__ == "__main__":
    unittest.main()
