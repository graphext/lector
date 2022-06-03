"""Detectors of CSV dialects (separator, quoting etc.)."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from csv import Dialect, Sniffer
from dataclasses import dataclass
from itertools import islice
from typing import Any, TextIO

try:
    import clevercsv as ccsv

    CLEVER_CSV = True
except Exception:
    CLEVER_CSV = False


N_ROWS_DFAULT: int = 100

DEFAULT_DIALECT: dict[str, Any] = {
    "delimiter": ",",
    "doublequote": True,  # read two consecutive quotechar elements INSIDE a field as single quote
    "escapechar": None,  # One-character string used to escape other characters.
    "lineterminator": "\r\n",
    "quotechar": '"',
    "quoting": 0,
    "skipinitialspace": False,  # Skip spaces after delimiter
}
"""Equals the default values in stdlib's csv module and pandas' read_csv options."""

DEFAULT_DELIMITERS: tuple[str] = (",", ";", "\t", "|")


def to_dict(dialect: Dialect) -> dict[str, Any]:
    return {attr: getattr(dialect, attr, None) for attr in DEFAULT_DIALECT}


@dataclass
class DialectDetector(ABC):
    @abstractmethod
    def detect(self, buffer: TextIO) -> dict[str, Any]:
        """Implement me."""


class StdLib(DialectDetector):
    """Use Python's built-in csv sniffer."""

    delimiters: Iterable[str] = DEFAULT_DELIMITERS
    n_rows: int = N_ROWS_DFAULT

    def detect(self, buffer: TextIO):

        pos = buffer.tell()

        sniffer = Sniffer()
        sniffer.preferred = []

        for n_rows in (self.n_rows, 1):

            try:
                buffer.seek(pos)
                sample = "\n".join(islice(buffer, n_rows))
                dialect = sniffer.sniff(sample, delimiters=self.delimiters)
                return to_dict(dialect)
            except Exception:
                pass

        print("Falling back to default dialect...")
        return DEFAULT_DIALECT


if CLEVER_CSV:

    class CleverCSV(DialectDetector):

        num_chars: int = int(1e6)
        skip: bool = True
        method: str = "normal"
        verbose: bool = False

        def detect(self, buffer: TextIO):
            text = buffer.read(self.num_chars)
            dialect = ccsv.Detector().detect(
                text, verbose=self.verbose, method=self.method, skip=self.skip
            )
            dialect = dialect.to_dict()
            dialect = {k: v if v != "" else None for k, v in dialect.items()}
            return {**DEFAULT_DIALECT, **dialect}
