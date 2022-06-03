"""Detectors of preambles in CSV files."""
from __future__ import annotations

from abc import ABC, abstractmethod
from itertools import islice
from typing import TextIO

N_ROWS_DFAULT: int = 100

QUOTE: str = '"'


class PreambleDetector(ABC):
    """Base class for detecting preambles (initial junk) in a CSV buffer."""

    def __init__(
        self,
        buffer: TextIO,
        n_rows: int = N_ROWS_DFAULT,
        separator: str | None = None,
    ) -> None:
        self.buffer = buffer
        self.separator = separator
        self.rows = [row.strip() for row in islice(buffer, n_rows)]

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

    # @classmethod
    # def items(cls) -> Iterator[tuple[str, type]]:
    #     for k, v in cls.DETECTORS.items():
    #         yield k, v

    @classmethod
    def detect(
        cls, buffer: TextIO, n_rows: int = N_ROWS_DFAULT, separator: str | None = None
    ) -> int | None:
        """Get first preamble detector matching the csv buffer."""
        pos = buffer.tell()

        for name, detcls in cls.DETECTORS.items():
            detector = detcls(buffer, n_rows=n_rows, separator=separator)
            skiprows = detector.detect()
            if skiprows:
                print(f"'{name}' preamble matches CSV buffer: detected {skiprows} rows to skip.")
                return skiprows

            buffer.seek(pos)

        return None


@Preambles.register
class Brandwatch(PreambleDetector):
    """Detect CSV files exported from Brandwatch.

    Brandwatch uses comma as separator and includes a row of commas only
    before the real csv starts.
    """

    def detect(self) -> int:
        for i, row in enumerate(self.rows):
            if len(row) > 0 and all(x == "," for x in row):
                return i + 1

        return 0


@Preambles.register
class Fieldless(PreambleDetector):
    """Detects initial rows that don't contain any delimited fields.

    Complicated by possibility of quoted multiline rows.
    """

    DELIMITERS = ("\t", ",", ";", "|")

    @classmethod
    def unescaped_quotes(cls, txt: str) -> list[int]:
        """Get inidices of quote chars that have no adjacent quote char."""

        def has_adj_quote(txt: str, i: int):
            if i == 0:
                return len(txt) > 1 and txt[1] == QUOTE
            elif i == len(txt) - 1:
                return txt[-2] == QUOTE
            else:
                return txt[i - 1] == QUOTE or txt[i + 1] == QUOTE

        ids = [i for i, c in enumerate(txt) if c == QUOTE and not has_adj_quote(txt, i)]
        return ids

    @classmethod
    def delimiter_count(cls, row: str) -> int:
        return len([c for c in row if c in cls.DELIMITERS])

    @classmethod
    def delimited(cls, row: str) -> bool:
        """Check if row has delimiter, i.e. more than one field."""
        if not row or cls.delimiter_count(row) == 0:
            return False

        n = len(cls.unescaped_quotes(row))
        if (n == 2) and (row[0] == QUOTE) and (row[-1] == QUOTE):
            # Single field row
            return False
        elif (n > 0) and (row[0] != QUOTE):
            # If a quoted field doesn't start a 1st character, there must have been
            # another, delimited field before it
            return True

        return n % 2 == 0

    @classmethod
    def starts_multiline(cls, row: str) -> bool:
        """Only needs to detect if row starts with an unmatched quote.

        If there is more than one unescaped double quote (i.e. multiple quoted fields), or it is not
        at the beginning (there is another field before the quoted one) then a valid CSV file would
        have to have separators between them.
        """
        return row.startswith(QUOTE) and len(cls.unescaped_quotes(row)) == 1

    @classmethod
    def stops_multiline(cls, row: str) -> bool:
        """Only needs to detect a closing double quote assuming that we're already inside a
        multiline string.

        Doesn't matter if there are more than one unescaped double quote. If there are multiple, a
        valid CSV file would have to have separators between them.
        """
        return len(cls.unescaped_quotes(row)) > 0

    def detect(self) -> int:
        """Iterate over rows, counting how many consecutive fieldless rows we have.

        If only some initial rows are fieldless, skip them. If none are fieldless, assume single
        column.
        """
        skip = 0
        multiline = None
        multiline_cnt = 0

        for row in self.rows:

            if not multiline:
                if self.starts_multiline(row):
                    multiline_cnt += 1
                    multiline = row
                elif self.delimited(row):
                    break
                else:
                    skip += 1
            else:
                multiline += row
                multiline_cnt += 1

                if self.stops_multiline(row):
                    delimited = self.delimited(multiline)

                    if not delimited:
                        skip += multiline_cnt
                    else:
                        break

                    multiline = None
                    multiline_cnt = 0
                else:
                    pass

        if skip and skip < len(self.rows):
            return skip

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
            is_report = any("informe de" in row.lower() for row in self.rows[0:skip])
            comma_sep_cols = self.rows[skip].split(",")
            tab_sep_cols = self.rows[skip].split("\t")
            has_campaign_col = any(
                "Campaña" in columns for columns in (comma_sep_cols, tab_sep_cols)
            )

            if is_report and has_campaign_col:
                self.skipfooter = 2
            else:
                skip = 0
                self.skipfooter = 0

        return skip
