"""File Reader and Writer for DuneRecords"""
from __future__ import annotations

import csv
import json
import logging
import os.path
from abc import ABC, abstractmethod
from os.path import exists
from pathlib import Path
from typing import TextIO, Callable, List, Tuple

# ndjson missing types: https://github.com/rhgrant10/ndjson/issues/10
import ndjson  # type: ignore

from dune_client.types import DuneRecord

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")


class FileRWInterface(ABC):
    """Interface for File Read, Write and Append functionality (specific to Dune Query Results)"""

    def __init__(self, path: Path | str, name: str, encoding: str = "utf-8"):
        self.path = path
        self.filename = name
        self.encoding = encoding

    @property
    def filepath(self) -> str:
        """Internal method for building absolute path."""
        return os.path.join(self.path, self.filename)

    @classmethod
    def from_str(
        cls, value: str, path: Path | str, name: str, encoding: str = "utf-8"
    ) -> FileRWInterface:
        """
        Constructs and instance of FileType from string
        This method is used by FileIO constructor,
        so that users don't have to import this class
        """
        lowered_value = value.lower()
        if "ndjson" in lowered_value:
            return NDJSONFile(path, name, encoding)
        if "json" in lowered_value:
            return JSONFile(path, name, encoding)
        if "csv" in lowered_value:
            return CSVFile(path, name, encoding)
        raise ValueError(f"Could not determine file type from {value}!")

    @abstractmethod
    def _assert_matching_keys(self, keys: Tuple[str, ...]) -> None:
        """Used as validation for append"""

    @abstractmethod
    def load(self, file: TextIO) -> list[DuneRecord]:
        """Loads DuneRecords from `file`"""

    @abstractmethod
    def write(
        self, out_file: TextIO, data: list[DuneRecord], skip_headers: bool = False
    ) -> None:
        """Writes `data` to `out_file`"""

    def append(self, data: List[DuneRecord]) -> None:
        """Appends `data` to file with `name`"""
        if len(data) > 0:
            self._assert_matching_keys(tuple(data[0].keys()))
        with open(self.filepath, "a+", encoding=self.encoding) as out_file:
            return self.write(out_file, data, skip_headers=True)


class JSONFile(FileRWInterface):
    """File Read/Writer for JSON format"""

    def _assert_matching_keys(self, keys: Tuple[str, ...]) -> None:
        with open(self.filepath, "r", encoding=self.encoding) as file:
            single_object = json.loads(file.readline())[0]
            existing_keys = single_object.keys()

        key_tuple = tuple(existing_keys)
        assert keys == key_tuple, f"{keys} != {key_tuple}"

    def load(self, file: TextIO) -> list[DuneRecord]:
        """Loads DuneRecords from `file`"""
        loaded_file: list[DuneRecord] = json.loads(file.read())
        return loaded_file

    def write(
        self, out_file: TextIO, data: list[DuneRecord], skip_headers: bool = False
    ) -> None:
        """Writes `data` to `out_file`"""
        out_file.write(json.dumps(data))

    def append(self, data: List[DuneRecord]) -> None:
        """Appends `data` to file with `name`"""
        if len(data) > 0:
            self._assert_matching_keys(tuple(data[0].keys()))
        with open(self.filepath, "r", encoding=self.encoding) as existing_file:
            existing_data = self.load(existing_file)
        with open(self.filepath, "w", encoding=self.encoding) as existing_file:
            self.write(existing_file, existing_data + data)


class NDJSONFile(FileRWInterface):
    """File Read/Writer for NDJSON format"""

    def _assert_matching_keys(self, keys: Tuple[str, ...]) -> None:

        with open(self.filepath, "r", encoding=self.encoding) as file:
            single_object = json.loads(file.readline())
            existing_keys = single_object.keys()

        key_tuple = tuple(existing_keys)
        assert keys == key_tuple, f"{keys} != {key_tuple}"

    def load(self, file: TextIO) -> list[DuneRecord]:
        """Loads DuneRecords from `file`"""
        return list(ndjson.reader(file))

    def write(
        self, out_file: TextIO, data: list[DuneRecord], skip_headers: bool = False
    ) -> None:
        """Writes `data` to `out_file`"""
        writer = ndjson.writer(out_file, ensure_ascii=False)
        for row in data:
            writer.writerow(row)


class CSVFile(FileRWInterface):
    """File Read/Writer for CSV format"""

    def _assert_matching_keys(self, keys: Tuple[str, ...]) -> None:
        with open(self.filepath, "r", encoding=self.encoding) as file:
            # Check matching headers.
            headers = file.readline()
            existing_keys = headers.strip().split(",")

        key_tuple = tuple(existing_keys)
        assert keys == key_tuple, f"{keys} != {key_tuple}"

    def load(self, file: TextIO) -> list[DuneRecord]:
        """Loads DuneRecords from `file`"""
        return list(csv.DictReader(file))

    def write(
        self, out_file: TextIO, data: list[DuneRecord], skip_headers: bool = False
    ) -> None:
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


class FileIO:
    """
    CSV is a more compact file type,
        but requires iteration over the set pre and post write
    JSON is a redundant file format
        but writes the content exactly as it is received from Dune.
    NDJSON used for data "streams"
    """

    def __init__(
        self,
        path: Path | str,
        encoding: str = "utf-8",
    ):
        if not os.path.exists(path):
            logger.info(f"creating write path {path}")
            os.makedirs(path)
        self.path = path
        self.encoding: str = encoding

    def _write(
        self,
        data: List[DuneRecord],
        writer: FileRWInterface,
        skip_empty: bool,
    ) -> None:
        # The following three lines are duplicated in _append, due to python version compatibility
        # https://github.com/cowprotocol/dune-client/issues/45
        # We will continue to support python < 3.10 until ~3.13, this issue will remain open.
        if skip_empty and len(data) == 0:
            logger.info(f"Nothing to write to {writer.filename}... skipping")
            return None
        with open(writer.filepath, "w", encoding=self.encoding) as out_file:
            writer.write(out_file, data)
        return None

    def _append(
        self,
        data: List[DuneRecord],
        writer: FileRWInterface,
        skip_empty: bool,
    ) -> None:
        fname = writer.filename
        if skip_empty and len(data) == 0:
            logger.info(f"Nothing to write to {fname}... skipping")
            return None
        if not exists(writer.filepath):
            logger.warning(
                f"File {fname} does not exist, using write instead of append!"
            )
            return self._write(data, writer, skip_empty)

        return writer.append(data)

    def append_csv(
        self,
        data: list[DuneRecord],
        name: str,
        skip_empty: bool = True,
    ) -> None:
        """Appends `data` to csv file `name`"""
        # This is a special case because we want to skip headers when the file already exists
        # Additionally, we may want to validate that the headers actually coincide.
        self._append(data, CSVFile(self.path, name, self.encoding), skip_empty)

    def append_json(
        self, data: list[DuneRecord], name: str, skip_empty: bool = True
    ) -> None:
        """
        Appends `data` to json file `name`
        This is the least efficient of all, since we have to load the entire file,
        concatenate the lists and then overwrite the file!
        Other filetypes such as CSV and NDJSON can be directly appended to!
        """
        self._append(data, JSONFile(self.path, name, self.encoding), skip_empty)

    def append_ndjson(
        self, data: list[DuneRecord], name: str, skip_empty: bool = True
    ) -> None:
        """Appends `data` to ndjson file `name`"""
        self._append(data, NDJSONFile(self.path, name, self.encoding), skip_empty)

    def write_csv(
        self, data: list[DuneRecord], name: str, skip_empty: bool = True
    ) -> None:
        """Writes `data` to csv file `name`"""
        self._write(data, CSVFile(self.path, name, self.encoding), skip_empty)

    def write_json(
        self, data: list[DuneRecord], name: str, skip_empty: bool = True
    ) -> None:
        """Writes `data` to json file `name`"""
        self._write(data, JSONFile(self.path, name, self.encoding), skip_empty)

    def write_ndjson(
        self, data: list[DuneRecord], name: str, skip_empty: bool = True
    ) -> None:
        """Writes `data` to ndjson file `name`"""
        self._write(data, NDJSONFile(self.path, name, self.encoding), skip_empty)

    def _load(self, reader: FileRWInterface) -> list[DuneRecord]:
        """Loads DuneRecords from file `name`"""
        with open(reader.filepath, "r", encoding=self.encoding) as file:
            return reader.load(file)

    def load_csv(self, name: str) -> list[DuneRecord]:
        """Loads DuneRecords from csv file `name`"""
        return self._load(CSVFile(self.path, name, self.encoding))

    def load_json(self, name: str) -> list[DuneRecord]:
        """Loads DuneRecords from json file `name`"""
        return self._load(JSONFile(self.path, name, self.encoding))

    def load_ndjson(self, name: str) -> list[DuneRecord]:
        """Loads DuneRecords from ndjson file `name`"""
        return self._load(NDJSONFile(self.path, name, self.encoding))

    def _parse_ftype(self, name: str, ftype: FileRWInterface | str) -> FileRWInterface:
        if isinstance(ftype, str):
            ftype = FileRWInterface.from_str(ftype, self.path, name, self.encoding)
        return ftype

    def load_singleton(
        self, name: str, ftype: FileRWInterface | str, index: int = 0
    ) -> DuneRecord:
        """Loads and returns single entry by index (default 0)"""
        reader = self._parse_ftype(name, ftype)
        return self._load(reader)[index]


WriteLikeSignature = Callable[[FileIO, List[DuneRecord], str, FileRWInterface], None]
