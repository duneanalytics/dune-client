import datetime
import unittest
from dune_client.util import get_package_version, age_in_hours


class TestUtils(unittest.TestCase):
    def test_package_version_some(self):
        version_string = get_package_version("requests")
        parsed_version = list(map(int, version_string.split(".")))
        self.assertGreaterEqual(parsed_version, [2, 31, 0])

    def test_package_version_none(self):
        # Can't self refer (this should only work when user has dune-client installed).
        self.assertIsNone(get_package_version("unittest"))

    def test_age_in_hours(self):
        march_ten_eighty_five = datetime.datetime(
            1985, 3, 10, tzinfo=datetime.timezone.utc
        )
        self.assertGreaterEqual(age_in_hours(march_ten_eighty_five), 314159)
