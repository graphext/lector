"""Test integration/interactions between encoding, preambles, dialect."""
import io

import pytest
from hypothesis import given
from hypothesis_csv.strategies import csv as make_csv

from lector.csv.abc import EmptyFileError, Reader

# from lector.log import LOG


class NonParser(Reader):
    def parse(self, *args, **kwds) -> None:
        return None


def analyze_string(csv: str):
    csv = io.StringIO(csv)
    reader = NonParser(csv)
    reader.read()
    return reader.format


def test_empty_file():
    with pytest.raises(EmptyFileError):
        analyze_string("")


@given(csv=make_csv(lines=10, header=5))
def test_dialect(csv: str):
    reader = analyze_string(csv)
    # dialect = reader.dialect
    assert len(reader.columns) == 5
