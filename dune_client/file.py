"""File Reader and Writer for DuneRecords"""
from __future__ import annotations

import csv
import json
import logging
import os.path
from enum import Enum
from pathlib import Path
from typing import TextIO

# ndjson missing types: https://github.com/rhgrant10/ndjson/issues/10
import ndjson  # type: ignore

from dune_client.types import DuneRecord

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")


class FileType(Enum):
    """
    Enum variants for supported file types
    CSV is the most space efficient (least redundant) file format,
    but some people like others.
    """

    CSV = ".csv"
    JSON = ".json"
    NDJSON = ".ndjson"

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def from_str(cls, value: str) -> FileType:
        """
        Constructs and instance of FileType from string
        This method is used by FileIO constructor,
        so that users don't have to import this class
        """
        lowered_value = value.lower()
        if "ndjson" in lowered_value:
            return cls.NDJSON
        if "json" in lowered_value:
            return cls.JSON
        if "csv" in lowered_value:
            return cls.CSV
        raise ValueError(f"Unrecognized FileType {value}")

    def load(self, file: TextIO) -> list[DuneRecord]:
        """Loads DuneRecords from file"""
        logger.debug(f"Attempting to loading results from file {file.name}")
        if self == FileType.JSON:
            loaded_file: list[DuneRecord] = json.loads(file.read())
            return loaded_file
        if self == FileType.CSV:
            return list(csv.DictReader(file))
        if self == FileType.NDJSON:
            return list(ndjson.reader(file))
        raise ValueError(f"Unrecognized FileType {self} for {file.name}")

    def write(self, out_file: TextIO, data: list[DuneRecord]) -> None:
        """Writes `data` to `out_file`"""
        logger.debug(f"writing results to file {out_file.name}")
        if self == FileType.CSV:
            headers = data[0].keys()
            data_tuple = [tuple(rec.values()) for rec in data]
            dict_writer = csv.DictWriter(out_file, headers, lineterminator="\n")
            dict_writer.writeheader()
            writer = csv.writer(out_file, lineterminator="\n")
            writer.writerows(data_tuple)

        elif self == FileType.JSON:
            out_file.write(json.dumps(data))

        elif self == FileType.NDJSON:
            writer = ndjson.writer(out_file, ensure_ascii=False)
            for row in data:
                writer.writerow(row)
        else:
            raise ValueError(f"Unrecognized FileType {self} for {out_file.name}")


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

    def _filepath(self, name: str, ftype: FileType) -> str:
        """Internal method for building absolute path."""
        return os.path.join(self.path, name + str(ftype))

    def _write(self, data: list[DuneRecord], name: str, ftype: FileType) -> None:
        # TODO - use @skip_empty decorator here. Couldn't get the types to work.
        if len(data) == 0:
            logger.info(f"Nothing to write to {name}... skipping")
            return
        with open(self._filepath(name, ftype), "w", encoding=self.encoding) as out_file:
            ftype.write(out_file, data)

    def write_csv(self, data: list[DuneRecord], name: str) -> None:
        """Writes `data` to csv file `name`"""
        self._write(data, name, FileType.CSV)

    def write_json(self, data: list[DuneRecord], name: str) -> None:
        """Writes `data` to json file `name`"""
        self._write(data, name, FileType.JSON)

    def write_ndjson(self, data: list[DuneRecord], name: str) -> None:
        """Writes `data` to ndjson file `name`"""
        self._write(data, name, FileType.NDJSON)

    def _load(self, name: str, ftype: FileType) -> list[DuneRecord]:
        """Loads DuneRecords from file `name`"""
        with open(self._filepath(name, ftype), "r", encoding=self.encoding) as file:
            return ftype.load(file)

    def load_csv(self, name: str) -> list[DuneRecord]:
        """Loads DuneRecords from csv file `name`"""
        return self._load(name, FileType.CSV)

    def load_json(self, name: str) -> list[DuneRecord]:
        """Loads DuneRecords from json file `name`"""
        return self._load(name, FileType.JSON)

    def load_ndjson(self, name: str) -> list[DuneRecord]:
        """Loads DuneRecords from ndjson file `name`"""
        return self._load(name, FileType.NDJSON)

    @staticmethod
    def _parse_ftype(ftype: FileType | str) -> FileType:
        if isinstance(ftype, str):
            ftype = FileType.from_str(ftype)
        return ftype

    def load_singleton(
        self, name: str, ftype: FileType | str, index: int = 0
    ) -> DuneRecord:
        """Loads and returns single entry by index (default 0)"""
        return self._load(name, self._parse_ftype(ftype))[index]
