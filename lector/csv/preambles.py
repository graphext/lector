"""Detectors of preambles in CSV files."""
from __future__ import annotations

import csv
import re
from abc import ABC, abstractmethod
from itertools import islice
from typing import TextIO

N_ROWS_DFAULT: int = 100

QUOTE: str = '"'

CONSECUTIVE_QUOTES = re.compile(r'"+')


class PreambleDetector(ABC):
    """Base class for detecting preambles (initial junk) in a CSV buffer."""

    def __init__(
        self,
        buffer: TextIO,
        n_rows: int = N_ROWS_DFAULT,
        delimiter: str = ",",
    ) -> None:
        self.buffer = buffer
        self.cursor = buffer.tell()
        self.n_rows = n_rows
        self.delimiter = delimiter

    @abstractmethod
    def detect(self) -> int:
        """Detect preamble and return number of lines to skip."""


class Preambles:
    """Registry to manage preamble detectors."""

    DETECTORS = {}

    @classmethod
    def register(cls, registered: type) -> type:
        cls.DETECTORS[registered.__name__] = registered
        return registered

    @classmethod
    def detect(cls, buffer: TextIO, n_rows: int = N_ROWS_DFAULT, delimiter: str = ",") -> int:
        """Get first preamble detector matching the csv buffer."""
        cursor = buffer.tell()

        for name, detcls in cls.DETECTORS.items():
            detector = detcls(buffer, n_rows=n_rows, delimiter=delimiter)
            skiprows = detector.detect()
            if skiprows:
                print(f"'{name}' preamble matches CSV buffer: detected {skiprows} rows to skip.")
                return skiprows

            buffer.seek(cursor)

        return 0


@Preambles.register
class Brandwatch(PreambleDetector):
    """Detect CSV files exported from Brandwatch.

    Brandwatch uses comma as separator and includes a row of commas only
    before the real csv starts.
    """

    def detect(self) -> int:
        self.buffer.seek(self.cursor)
        rows = [row.strip() for row in islice(self.buffer, self.n_rows)]

        for i, row in enumerate(rows):
            if len(row) > 0 and all(x == self.delimiter for x in row):
                return i + 1

        return 0


@Preambles.register
class Fieldless(PreambleDetector):
    """Detects initial rows that don't contain any delimited fields."""

    def detect(self) -> int:
        """Count how many consecutive initial fieldless rows we have."""
        self.buffer.seek(self.cursor)

        reader = csv.reader(
            islice(self.buffer, self.n_rows),
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            doublequote=True,
            skipinitialspace=True,
        )

        for row in reader:
            if len(row) > 1:
                return reader.line_num - 1

        return 0


@Preambles.register
class GoogleAds(Fieldless):
    """In GoogleAds CSVs the garbage lines don't contain the separator (comma or tab).

    The only complications are that 1) GoogleAds has two CSV export formats: 'Excel' using tabs
    as separators and normal 'CSV' the comma; 2) A single column CSV wouldn't have the
    separator either.

    GoogleAds also seems to include two "totals" rows at the end, which we exclude here.
    """

    def detect(self) -> int:

        skip = super().detect()

        if skip:

            self.buffer.seek(self.cursor)
            rows = [row.strip() for row in islice(self.buffer, self.n_rows)]

            is_report = any("informe de" in row.lower() for row in rows[0:skip])
            has_campaign_col = any("Campaña" in col for col in rows[skip].split(","))

            if is_report and has_campaign_col:
                self.skipfooter = 2
            else:
                skip = 0
                self.skipfooter = 0

        return skip
