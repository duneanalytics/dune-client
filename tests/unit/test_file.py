import os
import unittest

from dune_client.file import FileIO, FileType

TEST_FILE = "test"
TEST_PATH = "tmp"


def cleanup():
    for extension in [e.value for e in FileType]:
        try:
            os.remove(os.path.join(TEST_PATH, TEST_FILE + str(extension)))
        except FileNotFoundError:
            pass


def cleanup_files(func):
    """This decorator can be used for testing methods outside of this class"""

    def wrapped_func(self):
        func(self)
        cleanup()

    return wrapped_func


class TestFileIO(unittest.TestCase):
    """These tests indirectly test FileType's read and write functionality."""

    def setUp(self) -> None:
        self.dune_records = [
            {"col1": "value01", "col2": "value02"},
            {"col1": "value11", "col2": "value12"},
        ]

    def tearDown(self) -> None:
        cleanup()

    def test_write_and_load(self):
        file_manager = FileIO(TEST_PATH)
        for file_type in FileType:
            file_manager._write(self.dune_records, TEST_FILE, file_type)
            loaded_records = file_manager._load(TEST_FILE, file_type)
            self.assertEqual(self.dune_records, loaded_records)

    def test_load_singleton(self):
        file_manager = FileIO(TEST_PATH)
        for file_type in FileType:
            file_manager._write(self.dune_records, TEST_FILE, file_type)
            entry_0 = file_manager.load_singleton(TEST_FILE, file_type)
            entry_1 = file_manager.load_singleton(TEST_FILE, file_type, 1)
            self.assertEqual(self.dune_records[0], entry_0)
            self.assertEqual(self.dune_records[1], entry_1)

    def test_skip_empty_write(self):
        file_manager = FileIO(TEST_PATH)
        for file_type in FileType:
            with self.assertLogs():
                file_manager._write([], TEST_FILE, file_type)
            with self.assertRaises(FileNotFoundError):
                file_manager._load(TEST_FILE, file_type)

    def test_idempotent_write(self):
        file_manager = FileIO(TEST_PATH)
        for file_type in FileType:
            file_manager._write(self.dune_records, TEST_FILE, file_type)
            file_manager._write(self.dune_records, TEST_FILE, file_type)
            self.assertEqual(
                self.dune_records, file_manager._load(TEST_FILE, file_type)
            )

    def test_file_type(self):
        for enum_instance in FileType:
            self.assertEqual(enum_instance, FileType.from_str(str(enum_instance)))
        # The above is equivalent to, but also covers new items when added.
        # self.assertEqual(FileType.CSV, FileType.from_str("csv"))
        # self.assertEqual(FileType.JSON, FileType.from_str("json"))
        # self.assertEqual(FileType.NDJSON, FileType.from_str("ndjson"))
