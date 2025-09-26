import datetime
import unittest

from dune_client.util import age_in_hours, get_package_version


class TestUtils(unittest.TestCase):
    def test_package_version_some(self):
        version_string = get_package_version("requests")
        parsed_version = list(map(int, version_string.split(".")))
        assert parsed_version >= [2, 31, 0]

    def test_package_version_none(self):
        # Can't self refer (this should only work when user has dune-client installed).
        assert get_package_version("unittest") is None

    def test_age_in_hours(self):
        march_ten_eighty_five = datetime.datetime(1985, 3, 10, tzinfo=datetime.UTC)
        assert age_in_hours(march_ten_eighty_five) >= 314159
