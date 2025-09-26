import contextlib
import unittest
from pathlib import Path

import pytest

from dune_client.file.base import CSVFile, JSONFile, NDJSONFile
from dune_client.file.interface import FileIO

TEST_FILE = "test"
TEST_PATH = "tmp"

FILE_WRITERS = [
    CSVFile(TEST_PATH, TEST_FILE + ".csv"),
    NDJSONFile(TEST_PATH, TEST_FILE + ".ndjson"),
    JSONFile(TEST_PATH, TEST_FILE + ".json"),
]


def cleanup():
    for writer in FILE_WRITERS:
        with contextlib.suppress(FileNotFoundError):
            Path(writer.filepath).unlink()


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
        self.file_writers = FILE_WRITERS

    def tearDown(self) -> None:
        cleanup()

    def test_invertible_write_and_load(self):
        for writer in self.file_writers:
            self.file_manager._write(self.dune_records, writer, True)
            loaded_records = self.file_manager._load(writer)
            assert self.dune_records == loaded_records, f"test invertible failed on {writer}"

    def test_append_ok(self):
        for writer in self.file_writers:
            self.file_manager._write(self.dune_records, writer, True)
            self.file_manager._append(self.dune_records, writer, True)
            loaded_records = self.file_manager._load(writer)
            expected = self.dune_records + self.dune_records
            assert expected == loaded_records, f"append failed on {writer}"

    def test_append_calls_write_on_new_file(self):
        for writer in self.file_writers:
            with self.assertLogs(level="WARNING"):
                self.file_manager._append(self.dune_records, writer, True)

    def test_append_error(self):
        invalid_records = [{}]  # Empty dict has different keys than self.dune_records
        for writer in self.file_writers:
            self.file_manager._write(self.dune_records, writer, True)
            with pytest.raises(AssertionError):
                self.file_manager._append(invalid_records, writer, True)

    def test_load_singleton(self):
        for writer in self.file_writers:
            self.file_manager._write(self.dune_records, writer, True)
            entry_0 = self.file_manager.load_singleton(
                name="Doesn't matter is in the writer", ftype=writer
            )
            entry_1 = self.file_manager.load_singleton("Doesn't matter is in the writer", writer, 1)
            assert self.dune_records[0] == entry_0, f"failed on {writer} at index 0"
            assert self.dune_records[1] == entry_1, f"failed on {writer} at index 1"

        for extension in [".csv", ".json", ".ndjson"]:
            # Files were already written above.
            entry_0 = self.file_manager.load_singleton(TEST_FILE + extension, extension)
            entry_1 = self.file_manager.load_singleton(TEST_FILE + extension, extension, 1)
            assert self.dune_records[0] == entry_0, f"failed on {extension} (extension) at index 0"
            assert self.dune_records[1] == entry_1, f"failed on {extension} (extension) at index 1"

    def test_write_any_format_with_arbitrary_extension(self):
        weird_name = "weird_file.ext"
        weird_files = [
            NDJSONFile(TEST_PATH, weird_name),
            JSONFile(TEST_PATH, weird_name),
            CSVFile(TEST_PATH, weird_name),
        ]
        extensions = [
            "ndjson",
            "json",
            "csv",
        ]
        for weird_file, ext in zip(weird_files, extensions, strict=False):
            self.file_manager._write(self.dune_records, weird_file, True)
            self.file_manager._load(weird_file)
            entry_0 = self.file_manager.load_singleton(weird_name, ext)
            entry_1 = self.file_manager.load_singleton("meaningless string", weird_file, 1)
            assert self.dune_records[0] == entry_0, f"failed on {weird_file} at index 0"
            assert self.dune_records[1] == entry_1, f"failed on {weird_file} at index 1"

    def test_skip_empty_write(self):
        for writer in self.file_writers:
            with self.assertLogs():
                self.file_manager._write([], writer, True)
            with pytest.raises(FileNotFoundError):
                self.file_manager._load(writer)

    def test_not_skip_empty_when_specified(self):
        for writer in self.file_writers:
            if isinstance(writer, CSVFile):
                with self.assertLogs(level="WARNING"):
                    # CSV empty files won't have any headers!
                    self.file_manager._write([], writer, False)
            else:
                with self.assertNoLogs():
                    self.file_manager._write([], writer, False)

            self.file_manager._load(writer)

    def test_idempotent_write(self):
        for writer in self.file_writers:
            self.file_manager._write(self.dune_records, writer, True)
            self.file_manager._write(self.dune_records, writer, True)
            assert self.dune_records == self.file_manager._load(writer), (
                f"idempotent write failed on {writer}"
            )
