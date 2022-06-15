"""Test detection of dialects of otherwise valid CSV files."""
import io
from csv import QUOTE_MINIMAL, get_dialect

import pytest
from hypothesis import given
from hypothesis.strategies import data
from hypothesis_csv.strategies import csv as csv_strat

from lector.csv.dialects import Dialect, PySniffer

from .utils import equal


def fix_expected_dialect(dialect):
    dialect.line_terminator = "\r\n"  # Hardcoded in sniffer (not detectable)
    dialect.quoting = QUOTE_MINIMAL  # Hardcoded in sniffer (not detectable)
    return dialect


@pytest.mark.parametrize("dialect_name", ["excel", "excel-tab", "unix"])
def test_pydialect_roundtrip(dialect_name: str):
    attrs = [
        "delimiter",
        "doublequote",
        "escapechar",
        "lineterminator",
        "quotechar",
        "quoting",
        "skipinitialspace",
        "strict",
    ]
    d1 = get_dialect(dialect_name)
    d2 = Dialect.from_builtin(d1).to_builtin()
    assert all(getattr(d1, a, None) == getattr(d2, a, None) for a in attrs)


@pytest.mark.parametrize("dialect_name", ["excel", "excel-tab", "unix"])
def test_dialect_roundtrip(dialect_name: str):
    b = get_dialect(dialect_name)
    d1 = Dialect.from_builtin(b)
    d2 = Dialect.from_builtin(d1.to_builtin())
    assert equal(d1, d2)


@given(data=data())
@pytest.mark.parametrize("dialect", ["excel", "excel-tab", "unix"])
def test_dialects(dialect, data):
    strategy = csv_strat(dialect=dialect, lines=3, header=2)
    csv = data.draw(strategy)
    expected = Dialect.from_builtin(get_dialect(dialect))
    expected = fix_expected_dialect(expected)
    detected = PySniffer().detect(io.StringIO(csv))
    assert equal(expected, detected)
