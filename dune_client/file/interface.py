"""File Reader and Writer for DuneRecords"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

from dune_client.file.base import CSVFile, FileRWInterface, JSONFile, NDJSONFile
from dune_client.types import DuneRecord

logger = logging.getLogger(__name__)


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
        path_obj = Path(path)
        if not path_obj.exists():
            logger.info(f"creating write path {path}")
            path_obj.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.encoding: str = encoding

    def _write(
        self,
        data: list[DuneRecord],
        writer: FileRWInterface,
        skip_empty: bool,
    ) -> None:
        # The following three lines are duplicated in _append, due to python version compatibility
        # https://github.com/cowprotocol/dune-client/issues/45
        # We will continue to support python < 3.10 until ~3.13, this issue will remain open.
        if skip_empty and len(data) == 0:
            logger.info(f"Nothing to write to {writer.filename}... skipping")
            return
        with Path(writer.filepath).open("w", encoding=self.encoding) as out_file:
            writer.write(out_file, data)
        return

    def _append(
        self,
        data: list[DuneRecord],
        writer: FileRWInterface,
        skip_empty: bool,
    ) -> None:
        fname = writer.filename
        if skip_empty and len(data) == 0:
            logger.info(f"Nothing to write to {fname}... skipping")
            return None
        if not Path(writer.filepath).exists():
            logger.warning(f"File {fname} does not exist, using write instead of append!")
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

    def append_json(self, data: list[DuneRecord], name: str, skip_empty: bool = True) -> None:
        """
        Appends `data` to json file `name`
        This is the least efficient of all, since we have to load the entire file,
        concatenate the lists and then overwrite the file!
        Other filetypes such as CSV and NDJSON can be directly appended to!
        """
        self._append(data, JSONFile(self.path, name, self.encoding), skip_empty)

    def append_ndjson(self, data: list[DuneRecord], name: str, skip_empty: bool = True) -> None:
        """Appends `data` to ndjson file `name`"""
        self._append(data, NDJSONFile(self.path, name, self.encoding), skip_empty)

    def write_csv(self, data: list[DuneRecord], name: str, skip_empty: bool = True) -> None:
        """Writes `data` to csv file `name`"""
        self._write(data, CSVFile(self.path, name, self.encoding), skip_empty)

    def write_json(self, data: list[DuneRecord], name: str, skip_empty: bool = True) -> None:
        """Writes `data` to json file `name`"""
        self._write(data, JSONFile(self.path, name, self.encoding), skip_empty)

    def write_ndjson(self, data: list[DuneRecord], name: str, skip_empty: bool = True) -> None:
        """Writes `data` to ndjson file `name`"""
        self._write(data, NDJSONFile(self.path, name, self.encoding), skip_empty)

    def _load(self, reader: FileRWInterface) -> list[DuneRecord]:
        """Loads DuneRecords from file `name`"""
        with Path(reader.filepath).open(encoding=self.encoding) as file:
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
            lowered_value = ftype.lower()
            if "ndjson" in lowered_value:
                return NDJSONFile(self.path, name, self.encoding)
            if "json" in lowered_value:
                return JSONFile(self.path, name, self.encoding)
            if "csv" in lowered_value:
                return CSVFile(self.path, name, self.encoding)
            raise ValueError(f"Could not determine file type from {ftype}!")
        return ftype

    def load_singleton(self, name: str, ftype: FileRWInterface | str, index: int = 0) -> DuneRecord:
        """Loads and returns single entry by index (default 0)"""
        reader = self._parse_ftype(name, ftype)
        return self._load(reader)[index]


WriteLikeSignature = Callable[[FileIO, list[DuneRecord], str, FileRWInterface], None]
