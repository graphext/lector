"""Test integration/interactions between encoding, preambles, dialect."""
from __future__ import annotations

import io
from csv import get_dialect

import pytest
from hypothesis import given
from hypothesis.strategies import data
from hypothesis_csv.strategies import csv as csv_strat

from lector.csv import Dialect, EmptyFileError, Reader

from .test_dialects import fix_expected_dialect
from .test_encodings import CODECS, codecs_compatible
from .utils import equal

PREAMBLES = [
    # Brandwatch, the delimiter should be ignored in preamble, as long as a row of commas
    # separates the preamble from CSV table
    ("a{d}b{d}c\n,,", 2),
    ("some text\n,,", 2),
    ("a{d}b{d}c\nsome text\n,,", 3),
    # Fieldless
    # Start with header line having multiple (two) fields (delimited)
    ("abc{d}def", 0),
    ('"abc"{d}"def"', 0),
    ('"abc"{d}def', 0),
    ('abc{d} "123,456"', 0),
    ('"[cat1, cat2]"{d} "123,67"', 0),
    # Since the line isn't quoted as a whole, the delimiter creates two fields
    ('A line that has "something{d}in quotes" and then some', 0),
    ('A line that has ""something{d} in quotes"" and then some', 0),
    # Start with line having single field
    ("abc", 1),
    ('"abc, def"', 1),
    ('"The ""text,"" is double-quoted, and contains a comma"', 1),
    ('"A line that has ""something, in quotes"" and then some"', 1),
    ('"A line that has something, and then some more"', 1),
    ('"One line, spreading over\n multiple, lines"', 2),
    ("One line\nAnother line", 2),
    ('One line\nAnother line\n"And, a\nthird"', 4),
]


def with_delimiter(s, delim):
    if "{d}" in s:
        return s.format(d=delim)
    return s


class NonParser(Reader):
    """Simple implementation of abstract class that only infers format."""

    def parse(self, *args, **kwds) -> None:
        return None


def detect_format(csv: str | bytes):
    if isinstance(csv, str):
        buffer = io.StringIO(csv)
    else:
        buffer = io.BytesIO(csv)

    reader = NonParser(buffer, log=False)
    reader.read()
    return reader.format


def test_empty_file():
    with pytest.raises(EmptyFileError):
        detect_format("")


@given(data=data())
@pytest.mark.parametrize("codec", CODECS)
@pytest.mark.parametrize("preamble", PREAMBLES)
@pytest.mark.parametrize("dialect", ["excel", "excel-tab", "unix"])
def test_formats(codec, preamble, dialect, data):
    """Test parsing with different encoding, dialect and preamble."""

    pydialect = get_dialect(dialect)

    # Make premable compatible with dialect & generated csv
    preamble, skiprows = preamble
    preamble = with_delimiter(preamble, pydialect.delimiter)
    preamble = preamble.replace("\n", pydialect.lineterminator)
    if not pydialect.skipinitialspace:
        preamble = preamble.replace(f"{pydialect.delimiter} ", f"{pydialect.delimiter}")

    # Create the full, encoded CSV "file"
    strategy = csv_strat(dialect=pydialect, lines=3, header=2)
    csv = data.draw(strategy)
    csv = preamble + pydialect.lineterminator + csv

    try:
        encoded = csv.encode(codec)
        format = detect_format(encoded)

        # We have very short CSVs with preambles including the ñ, whose encoding
        # cannot always be detected correctly (would be easier with larger examples)
        assert codecs_compatible(codec, format.encoding, encoded, n_err_max=1)
        assert equal(skiprows, format.preamble, extra=csv)

        exp_dialect = fix_expected_dialect(Dialect.from_builtin(pydialect))
        assert equal(exp_dialect, format.dialect, extra=csv)

    except UnicodeEncodeError:
        pass
