"""Detectors of preambles in CSV files.

This is generally a chicken-and-egg-type situation. Do detect generic preambles robustly and
efficiently, it would really help to know the CSV dialect, or at least the delimiter. But to detect
the dialect/delimiter correctly, we need to ignore/(skip) the preamble. Detectors may therefore
rely on (somtimes) overly simplistic heuristics implicitly assuming a certain dialect.
"""
from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import islice
from typing import TextIO

from ..log import LOG


@dataclass
class PreambleDetector(ABC):
    """Base class for detecting preambles (initial junk) in a CSV buffer."""

    n_rows: int = 100

    @abstractmethod
    def detect(self, buffer: TextIO) -> int:
        """Detect preamble and return number of lines to skip."""


class Preambles:
    """Registry to manage preamble detectors."""

    DETECTORS = {}

    @classmethod
    def register(cls, registered: type) -> type:
        cls.DETECTORS[registered.__name__] = registered
        return registered

    @classmethod
    def detect(
        cls,
        buffer: TextIO,
        detectors: Iterable[PreambleDetector] | None = None,
        log: bool = False,
    ) -> int:
        """Get result of first preamble detector matching the csv buffer.

        Matching here means detecting more than 0 rows of preamble text, and result
        is the number of rows to skip.

        If no detectors are provided (as ordered sequence), all registered
        detector classes are tried in registered order and using default parameters.
        """
        cursor = buffer.tell()

        if detectors is None:
            detectors = (det() for det in cls.DETECTORS.values())

        for detector in detectors:
            skiprows = detector.detect(buffer)
            if skiprows:
                if log:
                    name = detector.__class__.__name__
                    msg = f"'{name}' matches CSV buffer: detected {skiprows} rows to skip."
                    LOG.info(msg)
                return skiprows

            buffer.seek(cursor)

        return 0


@Preambles.register
@dataclass
class Brandwatch(PreambleDetector):
    """Detect CSV files exported from Brandwatch.

    Brandwatch uses the comma as separator and includes a row of commas only
    to separate preamble texts from the CSV table as such.
    """

    def detect(self, buffer: TextIO) -> int:
        rows = [row.strip() for row in islice(buffer, self.n_rows)]

        for i, row in enumerate(rows):
            if len(row) > 0 and all(x == "," for x in row):
                return i + 1

        return 0


@Preambles.register
@dataclass
class Fieldless(PreambleDetector):
    """Detects initial rows that don't contain any delimited fields.

    Tries parsing buffer using Python's built-in csv functionality, assuming as delimiter the most
    frequent character amongst those configured via ``delimiters``. Given this delimiter, the parser
    detects N initial lines containing a single field only, followed by at least one line containing
    multiple fields. N is then the number of rows to skip.
    """

    delimiters: str | list[str] = field(default_factory=lambda: [",", ";", "\t"])

    def detect_with_delimiter(self, buffer: TextIO, delimiter: str) -> int:
        """Count how many consecutive initial fieldless rows we have given specific delimiter."""

        reader = csv.reader(
            islice(buffer, self.n_rows),
            delimiter=delimiter,
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            doublequote=True,
            skipinitialspace=True,
        )

        for row in reader:
            if len(row) > 1:
                return reader.line_num - 1

        return 0

    def detect(self, buffer: TextIO) -> int:
        """Count consecutive initial fieldless rows given the most frequent delimiter."""

        cursor = buffer.tell()
        delimiters = [self.delimiters] if isinstance(self.delimiters, str) else self.delimiters

        text = "".join(islice(buffer, self.n_rows))
        counts = {delim: text.count(delim) for delim in delimiters}
        delimiter = max(counts.items(), key=lambda item: item[1])[0]

        buffer.seek(cursor)
        return self.detect_with_delimiter(buffer, delimiter)


@Preambles.register
@dataclass
class GoogleAds(Fieldless):
    """In GoogleAds CSVs the garbage lines don't contain the separator (comma or tab).

    The only complications are that 1) GoogleAds has two CSV export formats: 'Excel' using tabs
    as separators and normal 'CSV' the comma; 2) A single column CSV wouldn't have the
    separator either.

    GoogleAds also seems to include two "totals" rows at the end, which we exclude here.
    """

    def detect(self, buffer: TextIO) -> int:
        cursor = buffer.tell()
        skip = super().detect(buffer)

        if skip:
            buffer.seek(cursor)
            rows = [row.strip() for row in islice(buffer, self.n_rows)]

            is_report = any("informe de" in row.lower() for row in rows[0:skip])
            has_campaign_col = any("Campaña" in col for col in rows[skip].split(","))

            if is_report and has_campaign_col:
                self.skipfooter = 2
            else:
                skip = 0
                self.skipfooter = 0

        return skip
