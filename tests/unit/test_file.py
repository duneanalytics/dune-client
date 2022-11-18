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
    """This decorator can be used for testing methods outside this class"""

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
        self.file_manager = FileIO(TEST_PATH)

    def tearDown(self) -> None:
        cleanup()

    def test_invertible_write_and_load(self):
        for ftype in FileType:
            self.file_manager._write(self.dune_records, TEST_FILE, ftype)
            loaded_records = self.file_manager._load(TEST_FILE, ftype)
            self.assertEqual(
                self.dune_records,
                loaded_records,
                f"Assert invertible failed on {ftype}",
            )

    def test_append_ok(self):
        for ftype in FileType:
            self.file_manager._write(self.dune_records, TEST_FILE, ftype)
            self.file_manager._append(self.dune_records, TEST_FILE, ftype)
            loaded_records = self.file_manager._load(TEST_FILE, ftype)
            expected = self.dune_records + self.dune_records
            self.assertEqual(
                expected,
                loaded_records,
                f"test_append failed on {ftype}",
            )

    def test_append_calls_write_on_new_file(self):
        for ftype in FileType:
            with self.assertLogs(level="WARNING"):
                self.file_manager._append(self.dune_records, TEST_FILE, ftype)

    def test_append_error(self):
        invalid_records = [{}]  # Empty dict has different keys than self.dune_records
        for ftype in FileType:
            self.file_manager._write(self.dune_records, TEST_FILE, ftype)
            with self.assertRaises(AssertionError):
                self.file_manager._append(invalid_records, TEST_FILE, ftype)

    def test_load_singleton(self):
        for file_type in FileType:
            self.file_manager._write(self.dune_records, TEST_FILE, file_type)
            entry_0 = self.file_manager.load_singleton(TEST_FILE, file_type)
            entry_1 = self.file_manager.load_singleton(TEST_FILE, file_type, 1)
            self.assertEqual(
                self.dune_records[0],
                entry_0,
                f"load_singletons failed on {file_type} at index 0",
            )
            self.assertEqual(
                self.dune_records[1],
                entry_1,
                f"load_singletons failed on {file_type} at index 1",
            )

    def test_skip_empty_write(self):
        for file_type in FileType:
            with self.assertLogs():
                self.file_manager._write([], TEST_FILE, file_type)
            with self.assertRaises(FileNotFoundError):
                self.file_manager._load(TEST_FILE, file_type)

    def test_idempotent_write(self):
        for file_type in FileType:
            self.file_manager._write(self.dune_records, TEST_FILE, file_type)
            self.file_manager._write(self.dune_records, TEST_FILE, file_type)
            self.assertEqual(
                self.dune_records,
                self.file_manager._load(TEST_FILE, file_type),
                f"idempotent write failed on {file_type}",
            )

    def test_file_type(self):
        for file_type in FileType:
            self.assertEqual(
                file_type, FileType.from_str(str(file_type)), "failed on {file_type}"
            )
