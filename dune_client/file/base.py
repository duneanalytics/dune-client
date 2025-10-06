"""File Reader and Writer for DuneRecords"""

from __future__ import annotations

import csv
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

# ndjson missing types: https://github.com/rhgrant10/ndjson/issues/10
import ndjson  # type: ignore

if TYPE_CHECKING:
    from dune_client.types import DuneRecord

logger = logging.getLogger(__name__)


class FileRWInterface(ABC):
    """Interface for File Read, Write and Append functionality (specific to Dune Query Results)"""

    def __init__(self, path: Path | str, name: str, encoding: str = "utf-8"):
        self.path = path
        self.filename = name
        self.encoding = encoding

    @property
    def filepath(self) -> str:
        """Internal method for building absolute path."""
        return str(Path(self.path) / self.filename)

    @abstractmethod
    def _assert_matching_keys(self, keys: tuple[str, ...]) -> None:
        """Used as validation for append"""

    @abstractmethod
    def load(self, file: TextIO) -> list[DuneRecord]:
        """Loads DuneRecords from `file`"""

    @abstractmethod
    def write(self, out_file: TextIO, data: list[DuneRecord], skip_headers: bool = False) -> None:
        """Writes `data` to `out_file`"""

    def append(self, data: list[DuneRecord]) -> None:
        """Appends `data` to file with `name`"""
        if len(data) > 0:
            self._assert_matching_keys(tuple(data[0].keys()))
        with Path(self.filepath).open("a+", encoding=self.encoding) as out_file:
            return self.write(out_file, data, skip_headers=True)


class CSVFile(FileRWInterface):
    """File Read/Writer for CSV format"""

    def _assert_matching_keys(self, keys: tuple[str, ...]) -> None:
        with Path(self.filepath).open(encoding=self.encoding) as file:
            # Check matching headers.
            headers = file.readline()
            existing_keys = headers.strip().split(",")

        key_tuple = tuple(existing_keys)
        assert keys == key_tuple, f"{keys} != {key_tuple}"

    def load(self, file: TextIO) -> list[DuneRecord]:
        """Loads DuneRecords from `file`"""
        return list(csv.DictReader(file))

    def write(self, out_file: TextIO, data: list[DuneRecord], skip_headers: bool = False) -> None:
        """Writes `data` to `out_file`"""
        if len(data) == 0:
            logger.warning(
                "Writing an empty CSV file with headers -- will not work with append later."
            )
            return
        headers = data[0].keys()
        data_tuple = [tuple(rec.values()) for rec in data]
        dict_writer = csv.DictWriter(out_file, headers, lineterminator="\n")
        if not skip_headers:
            dict_writer.writeheader()
        writer = csv.writer(out_file, lineterminator="\n")
        writer.writerows(data_tuple)


class JSONFile(FileRWInterface):
    """File Read/Writer for JSON format"""

    def _assert_matching_keys(self, keys: tuple[str, ...]) -> None:
        with Path(self.filepath).open(encoding=self.encoding) as file:
            single_object = json.loads(file.readline())[0]
            existing_keys = single_object.keys()

        key_tuple = tuple(existing_keys)
        assert keys == key_tuple, f"{keys} != {key_tuple}"

    def load(self, file: TextIO) -> list[DuneRecord]:
        """Loads DuneRecords from `file`"""
        loaded_file: list[DuneRecord] = json.loads(file.read())
        return loaded_file

    def write(self, out_file: TextIO, data: list[DuneRecord], skip_headers: bool = False) -> None:
        """Writes `data` to `out_file`"""
        out_file.write(json.dumps(data))

    def append(self, data: list[DuneRecord]) -> None:
        """Appends `data` to file with `name`"""
        if len(data) > 0:
            self._assert_matching_keys(tuple(data[0].keys()))
        with Path(self.filepath).open(encoding=self.encoding) as existing_file:
            existing_data = self.load(existing_file)
        with Path(self.filepath).open("w", encoding=self.encoding) as existing_file:
            self.write(existing_file, existing_data + data)


class NDJSONFile(FileRWInterface):
    """File Read/Writer for NDJSON format"""

    def _assert_matching_keys(self, keys: tuple[str, ...]) -> None:
        with Path(self.filepath).open(encoding=self.encoding) as file:
            single_object = json.loads(file.readline())
            existing_keys = single_object.keys()

        key_tuple = tuple(existing_keys)
        assert keys == key_tuple, f"{keys} != {key_tuple}"

    def load(self, file: TextIO) -> list[DuneRecord]:
        """Loads DuneRecords from `file`"""
        return list(ndjson.reader(file))

    def write(self, out_file: TextIO, data: list[DuneRecord], skip_headers: bool = False) -> None:
        """Writes `data` to `out_file`"""
        writer = ndjson.writer(out_file, ensure_ascii=False)
        for row in data:
            writer.writerow(row)
