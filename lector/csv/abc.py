"""Fast and robust parser for potentially messy CSV data."""
from __future__ import annotations

import io
from abc import ABC, abstractmethod
from csv import DictReader
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, TextIO, Union

from rich.table import Table as RichTable

from ..log import LOG, dict_view, pformat
from . import dialects, encodings
from .dialects import Dialect, DialectDetector
from .encodings import EncodingDetector
from .preambles import Preambles

FileLike = Union[str, Path, IO]

PreambleRegistry = type(Preambles)


class EmptyFileError(Exception):
    """Raised when a binary file read() returns 0 bytes."""


def is_empty(buffer: IO) -> bool:
    """Check if a binary or text buffer is empty (from current position onwards)."""
    pos = buffer.tell()
    empty = len(buffer.read(1)) == 0
    buffer.seek(pos)
    return empty


@dataclass
class Format:
    """Holds all parameters needed to successfully read a CSV file."""

    encoding: str | None = "utf-8"
    preamble: int | None = 0
    dialect: Dialect | None = field(default_factory=lambda: Dialect())
    columns: list[str] | None = None

    def __rich__(self) -> RichTable:
        return dict_view(
            {k: v for k, v in self.__dict__.items() if k != "columns"},
            title="CSV Format",
            width=120,
        )


class Reader(ABC):
    """Base class for CSV readers."""

    def __init__(
        self,
        fp: FileLike,
        encoding: str | EncodingDetector | None = None,
        dialect: dict | DialectDetector | None = None,
        preamble: int | PreambleRegistry | None = None,
        log: bool = True,
    ) -> None:
        self.fp = fp
        self.encoding = encoding or encodings.Chardet()
        self.dialect = dialect or dialects.PySniffer()
        self.preamble = preamble or Preambles
        self.log = log

    def decode(self, fp: FileLike) -> TextIO:
        """Make sure we have a text buffer."""
        buffer = fp

        if isinstance(buffer, (str, Path)):
            if isinstance(self.encoding, str):
                buffer = open(buffer, "r", encoding=self.encoding, errors="replace")
            else:
                buffer = open(buffer, "rb")

        if is_empty(buffer):
            raise EmptyFileError(f"The passed object ({buffer}) contained 0 bytes of data.")

        if isinstance(buffer, io.BufferedIOBase):
            if isinstance(self.encoding, EncodingDetector):
                self.encoding = self.encoding.detect(buffer)
                buffer.seek(0)

            buffer = io.TextIOWrapper(buffer, encoding=self.encoding, errors="replace")
        else:
            self.encoding = buffer.encoding or "UTF-8"

        return buffer

    def detect_preamble(self, buffer: TextIO) -> int:
        """Detect the number of junk lines at the start of the file."""
        if self.preamble is None:
            return 0
        elif isinstance(self.preamble, (int, float)):
            return self.preamble
        elif issubclass(self.preamble, Preambles):
            return Preambles.detect(buffer, log=self.log) or 0

    def detect_dialect(self, buffer: TextIO) -> dict:
        """Detect separator, quote character etc."""
        if self.dialect is None:
            return Dialect()
        elif isinstance(self.dialect, DialectDetector):
            return self.dialect.detect(buffer)

    @classmethod
    def detect_columns(cls, buffer: TextIO, dialect: Dialect) -> list[str]:
        """Extract column names from buffer pointing at header row."""
        reader = DictReader(buffer, dialect=dialect.to_builtin())
        try:
            _ = next(reader)
        except StopIteration:
            pass
        return reader.fieldnames

    def analyze(self):
        """Infer all parameters required for reading a csv file."""

        self.buffer = self.decode(self.fp)
        self.buffer.seek(0)

        self.preamble = self.detect_preamble(self.buffer)
        self.buffer.seek(0)

        for _ in range(self.preamble):
            self.buffer.readline()

        header = self.buffer.tell()
        self.dialect = self.detect_dialect(self.buffer)

        self.buffer.seek(header)
        self.columns = self.detect_columns(self.buffer, self.dialect)
        self.buffer.seek(0)

        self.format = Format(
            encoding=self.encoding,
            preamble=self.preamble,
            dialect=self.dialect,
            columns=self.columns,
        )

        if self.log:
            LOG.info(pformat(self.format))

    @abstractmethod
    def parse(self, *args, **kwds) -> Any:
        """Parse the file pointer or text buffer. Args are forwarded to read()."""

    def read(self, *args, **kwds) -> Any:
        try:
            self.analyze()
            result = self.parse(*args, **kwds)
            self.buffer.close()
            return result
        except Exception:
            raise

    __call__ = read
