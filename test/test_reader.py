"""Test CSV readers."""
import io
from csv import get_dialect

import pytest
from hypothesis import given
from hypothesis.strategies import data
from hypothesis_csv.strategies import csv as csv_strat

from lector.csv import ArrowReader, Dialect, EmptyFileError

from .test_dialects import fix_expected_dialect
from .test_encodings import CODECS, codecs_compatible
from .test_formats import PREAMBLES, with_delimiter
from .utils import equal

SHAPES = [
    # (0, 1), # hypothesis_csv cannot generate a 0 row file
    # (1, 0), # This produces an empty file ("\r\n\r\n")
    (1, 1),
    (1, 3),
    (3, 1),
    (10, 5),
]
"""Different combinations of n_rows and n_cols."""

EMPTY = ["", "\n", "\n\n", "\r\n"]


@pytest.mark.parametrize("csv", EMPTY)
def test_empty(csv: str):
    """Correct number of columns and rows."""
    csv = io.BytesIO(csv.encode("utf-8"))
    with pytest.raises(EmptyFileError):
        ArrowReader(csv, log=False).read()


# @given(data=data())
# @pytest.mark.parametrize("shape", SHAPES)
# def test_parsed_shapes(shape, data):
#     n_rows, n_cols = shape
#     strategy = make_csv(lines=n_rows, header=n_cols)
#     csv = data.draw(strategy)
#     tbl = arrow_reader(csv).read()

#     assert len(tbl) == n_rows


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
        pass
