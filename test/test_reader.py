"""Test CSV readers."""
import csv
import io
import sys
from csv import get_dialect

import pytest
from hypothesis import given
from hypothesis.strategies import data
from hypothesis_csv.strategies import csv as csv_strat

import lector
from lector.csv import ArrowReader, Dialect, EmptyFileError

from .test_dialects import fix_expected_dialect
from .test_encodings import CODECS, codecs_compatible
from .test_formats import PREAMBLES, with_delimiter
from .utils import equal

SHAPES = [
    # (0, 1), # hypothesis_csv cannot generate a 0 row file  # noqa
    # (1, 0), # This produces an empty file ("\r\n\r\n")  # noqa
    (1, 1),
    (1, 3),
    (3, 1),
    (10, 5),
]
"""Different combinations of n_rows and n_cols."""

EMPTY = ["", "\n", "\n\n", "\r\n"]

NULL_BYTES_CSV = b"""
col1,col_\0_2,col3
1,abc,x
2,de\0fg,y
"""

UNNAMED = b"""
col1,,col3,,col5
1,2,3,4,5
6,7,8,9,10
"""


@pytest.mark.parametrize("csv", EMPTY)
def test_empty(csv: str):
    fp = io.BytesIO(csv.encode("utf-8"))
    with pytest.raises(EmptyFileError):
        ArrowReader(fp, log=False).read()


def test_unnamed():
    """Automatic names for unnnamed columns"""
    fp = io.BytesIO(UNNAMED)
    tbl = lector.read_csv(fp)
    assert tbl.column_names == ["col1", "Unnamed_0", "col3", "Unnamed_1", "col5"]


def test_null_bytes():
    """For now, null bytes don't throw error, but are also not removed automatically!"""

    with pytest.raises(csv.Error):
        # python's csv reader throws error on null byte
        s = io.StringIO(NULL_BYTES_CSV.decode("utf-8"))
        print("Null-byte CSV:", list(csv.reader(s)))

    tbl = lector.read_csv(io.BytesIO(NULL_BYTES_CSV))
    assert tbl.column_names == ["col1", "col_\x00_2", "col3"]
    assert tbl.column("col_\x00_2").to_pylist() == ["abc", "de\x00fg"]


@given(data=data())
@pytest.mark.parametrize("codec", CODECS)
@pytest.mark.parametrize("preamble", PREAMBLES)
@pytest.mark.parametrize("dialect", ["excel", "excel-tab", "unix"])
def test_all(codec, preamble, dialect, data):
    """Test parsing with different encoding, dialect and preamble."""

    pydialect = get_dialect(dialect)

    # Make premable compatible with dialect & generated csv
    preamble, skiprows = preamble
    preamble = with_delimiter(preamble, pydialect.delimiter)
    preamble = preamble.replace("\n", pydialect.lineterminator)
    if not pydialect.skipinitialspace:
        preamble = preamble.replace(f"{pydialect.delimiter} ", f"{pydialect.delimiter}")

    # Create the full, encoded CSV "file"
    n_lines = 3
    n_columns = 2
    strategy = csv_strat(dialect=pydialect, lines=n_lines, header=n_columns)
    csv = data.draw(strategy)
    csv = preamble + pydialect.lineterminator + csv

    try:
        encoded = csv.encode(codec)
        reader = ArrowReader(io.BytesIO(encoded), log=False)
        tbl = reader.read()

        # If a preamble test case specifies 0 as the number of skiprows, it means the preamble text
        # consists of a single line that should be interpreted as the header. In this case, the
        # header (first line) of the csv created automatically will effectively become another row
        # in the table.
        exp_num_rows = n_lines if skiprows > 0 else n_lines + 1
        assert equal(exp_num_rows, tbl.num_rows, extra=csv)
        assert equal(2, tbl.num_columns, extra=csv)

        # We have very short CSVs with preambles including the ñ, whose encoding
        # cannot always be detected correctly (would be easier with larger examples)
        assert codecs_compatible(codec, reader.format.encoding, encoded, n_err_max=1)
        assert equal(skiprows, reader.format.preamble, extra=csv)

        exp_dialect = fix_expected_dialect(Dialect.from_builtin(pydialect))
        assert equal(exp_dialect, reader.format.dialect, extra=csv)

    except UnicodeEncodeError:
        print(f"FAILED ON CSV:\n{csv}")
        sys.exit()
        raise
