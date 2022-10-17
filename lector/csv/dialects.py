"""Detectors of CSV dialects (separator, quoting etc.).

Note that python.csv is not even internally consistent. E.g. although the dialect used to produce a
CSV may specify ``\\n`` as the line terminator, the python sniffer is hard-coded to return
``\\r\\n`` (it doesn't actually support detecting it). It's own reader (and others hopefully) deal
internally with different line breaks, but it means one cannot compare a dialect used to generate a
CSV and a dialect created by sniffing the same (quoting is equally hard-coded to ``QUOTE_MINIMAL``).

Python quoting levels:

- ``QUOTE_ALL``: 1
- ``QUOTE_MINIMAL``: 0
- ``QUOTE_NONE``: 3
- ``QUOTE_NONNUMERIC``: 2

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from csv import QUOTE_MINIMAL, QUOTE_NONE
from csv import Dialect as PyDialect
from csv import Sniffer, get_dialect
from dataclasses import dataclass
from itertools import islice
from typing import TextIO

from ..log import LOG

try:
    import clevercsv as ccsv

    CLEVER_CSV = True
except Exception:
    CLEVER_CSV = False

PyDialectT = type(PyDialect)

N_ROWS_DFAULT: int = 100
"""How many rows to use for dialect detection."""

DELIMITER_OPTIONS: tuple[str] = (",", ";", "\t", "|")
"""Allowed delimiters for dialect detection."""


@dataclass
class Dialect:
    """A more convenient class for dialects than Python's built-in.

    The built-in Dialect is a class with class attributes only, and so instead of instances
    of that class, Python wants you to send references to subclasses around, which is, uhm,
    awkward to say the least (see below _to_builtin() for an example).
    """

    delimiter: str = ","
    quote_char: str = '"'
    escape_char: str | None = None
    double_quote: bool = True
    skip_initial_space: bool = False
    line_terminator: str = "\r\n"  # Default in Python and correct according to official spec
    quoting: int = QUOTE_MINIMAL

    @classmethod
    def from_builtin(cls, dialect: str | PyDialectT) -> Dialect:
        """Make an instance from a built-in dialect class."""
        if isinstance(dialect, str):
            dialect = get_dialect(dialect)

        return Dialect(
            delimiter=dialect.delimiter,
            quote_char=dialect.quotechar,
            escape_char=dialect.escapechar,
            double_quote=dialect.doublequote,
            skip_initial_space=dialect.skipinitialspace,
            line_terminator=dialect.lineterminator,
            quoting=dialect.quoting,
        )

    def to_builtin(self) -> PyDialectT:
        """Make a subclass of built-in Dialect from this instance."""

        class _Dialect(PyDialect):
            _name = "generated"
            lineterminator = self.line_terminator
            quoting = self.quoting
            escapechar = self.escape_char or None
            doublequote = self.double_quote
            delimiter = self.delimiter
            quotechar = self.quote_char
            skipinitialspace = self.skip_initial_space
            strict = False

        return _Dialect


@dataclass
class DialectDetector(ABC):
    """Base class for all dialect detectors."""

    @abstractmethod
    def detect(self, buffer: TextIO) -> Dialect:
        ...


@dataclass
class PySniffer(DialectDetector):
    """Use Python's built-in csv sniffer."""

    delimiters: Iterable[str] = DELIMITER_OPTIONS
    n_rows: int = N_ROWS_DFAULT
    log: bool = False

    def detect(self, buffer: TextIO) -> Dialect:
        """Detect a dialect we can read(!) a CSV with using the python sniffer.

        Note that the sniffer is not reliable for detecting quoting, quotechar etc., but reasonable
        defaults are almost guaranteed to work with most parsers. E.g. the lineterminator is not
        even configurable in pyarrow's csv reader, nor in pandas (python engine).
        """

        pos = buffer.tell()
        sniffer = Sniffer()
        sniffer.preferred = []

        for n_rows in (self.n_rows, 1):

            try:
                buffer.seek(pos)
                sample = "\n".join(islice(buffer, n_rows))
                dialect = sniffer.sniff(sample, delimiters=self.delimiters)

                # To read(!) a CSV reliably, we must have either doublequote=True or an escapechar,
                # yet Python's sniffer may return doublequote=False and no escapechar if nothing
                # was escaped in any way in the given CSV.
                dialect.doublequote = False if dialect.escapechar is not None else True

                # The lineterminator is always returned as "\r\n", but that's ok since parsers
                # tend to ignore it anyways
                # dialect.lineterminator = ...

                # May detect that sample has no quotes, but if correct, parsing with minimal quote
                # option will still work, and if detection was erroneous, assuming minimal quoting
                # is more robust. It's also the default in pandas (=0) and arrow ignores it.
                if dialect.quoting == QUOTE_NONE:
                    dialect.quoting = QUOTE_MINIMAL

                return Dialect.from_builtin(dialect)
            except Exception:
                pass

        if self.log:
            LOG.info("Falling back to default dialect...")
        return Dialect()


if CLEVER_CSV:

    @dataclass
    class CleverCSV(DialectDetector):
        """A more advanced dialect detector using CleverCsv."""

        num_chars: int = int(1e6)
        skip: bool = True
        method: str = "normal"
        verbose: bool = False

        def detect(self, buffer: TextIO) -> Dialect:
            text = buffer.read(self.num_chars)
            dialect = ccsv.Detector().detect(
                text, verbose=self.verbose, method=self.method, skip=self.skip
            )
            return Dialect.from_builtin(dialect.to_csv_dialect())
