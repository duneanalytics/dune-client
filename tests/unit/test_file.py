import os
import unittest

from dune_client.file import FileIO, FileType

TEST_FILE = "test"
TEST_PATH = "tmp"


def cleanup_files(func):
    def wrapped_func(self):
        func(self)
        for extension in [e.value for e in FileType]:
            try:
                os.remove(os.path.join(TEST_PATH, TEST_FILE + str(extension)))
            except FileNotFoundError:
                pass

    return wrapped_func


class TestFileIO(unittest.TestCase):
    """These tests indirectly test FileType's read and write functionality."""

    def setUp(self) -> None:
        self.dune_records = [
            {"col1": "value01", "col2": "value02"},
            {"col1": "value11", "col2": "value12"},
        ]
        self.csv_manager = FileIO(TEST_PATH, FileType.CSV)
        self.json_manager = FileIO(TEST_PATH, FileType.JSON)
        self.ndjson_type = FileIO(TEST_PATH, FileType.NDJSON)
        self.file_managers = [
            self.csv_manager,
            self.json_manager,
            self.ndjson_type,
        ]

    @cleanup_files
    def test_write_and_load(self):
        for file_manager in self.file_managers:
            file_manager.write(self.dune_records, TEST_FILE)
            loaded_records = file_manager.load(TEST_FILE)
            self.assertEqual(self.dune_records, loaded_records)

    def test_skip_empty_write(self):
        for file_manager in self.file_managers:
            with self.assertLogs():
                file_manager.write([], TEST_FILE)
            with self.assertRaises(FileNotFoundError):
                file_manager.load(TEST_FILE)

    def test_file_type(self):
        for enum_instance in FileType:
            self.assertEqual(enum_instance, FileType.from_str(str(enum_instance)))
        # The above is equivalent to, but also covers new items when added.
        # self.assertEqual(FileType.CSV, FileType.from_str("csv"))
        # self.assertEqual(FileType.JSON, FileType.from_str("json"))
        # self.assertEqual(FileType.NDJSON, FileType.from_str("ndjson"))
