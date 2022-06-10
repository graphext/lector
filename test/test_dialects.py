"""Test detection of dialects of otherwise valid CSV files."""
import io
from csv import QUOTE_MINIMAL, get_dialect

import pytest
from hypothesis import given
from hypothesis.strategies import data
from hypothesis_csv.strategies import csv as make_csv
from rich import inspect

from lector.csv.dialects import Dialect, PySniffer


def equal(obj1, obj2):
    eq = obj1 == obj2
    if not eq:
        inspect(obj1)
        inspect(obj2)
        return False
    return True


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


def test_simple_detector(simple_csv):
    detector = PySniffer()
    csv = io.StringIO(simple_csv.csv)
    expected = simple_csv.format.dialect
    detected = detector.detect(csv)
    assert equal(expected, detected)


@given(data=data())
@pytest.mark.parametrize("dialect", ["excel", "excel-tab", "unix"])
def test_dialects(dialect, data):
    strategy = make_csv(dialect=dialect, lines=3, header=2)
    csv = data.draw(strategy)
    expected = Dialect.from_builtin(get_dialect(dialect))

    # Adjust expected based on python sniffer's pecularities
    expected.line_terminator = "\r\n"  # Hardcoded in sniffer (not detectable)
    expected.quoting = QUOTE_MINIMAL  # Hardcoded in sniffer (not detectable)

    detected = PySniffer().detect(io.StringIO(csv))
    assert equal(expected, detected)
