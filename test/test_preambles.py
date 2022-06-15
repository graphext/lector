import io

import pytest
from hypothesis import given
from hypothesis.strategies import data
from hypothesis_csv.strategies import csv as csv_strat

from lector.csv.preambles import Brandwatch, Fieldless, GoogleAds, Preambles

from .utils import equal

BRANDWATCH_PREAMBLES = [
    ("a,b,c", 0),
    ("a,b,c\n,,", 2),
    ("some text\n,,", 2),
    ("a,b,c\nsome text\n,,", 3),
]
"""Brandwatch uses a line of delimiters only to mark end of preamble."""

FIELDLESS_PREAMBLES = [
    # Start with line having multiple fields
    ("abc,def", 0),
    ('"abc","def"', 0),
    ('"abc",def', 0),
    ('abc, "123,456"', 0),
    ('"[cat1, cat2]", "123,67"', 0),
    ('A line that has "something,in quotes" and then some', 0),
    ('A line that has ""something, in quotes"" and then some', 0),
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
"""Test cases for Fieldless detector, ints indicating how many lines to skip for preamble."""


GOOGLEADS_PREAMBLES = [
    ("Informe de something something\nSome other info\nCampaña, Column", 2),
    ("Informe de something something\nCampaña, Column", 1),
    ("Something something\nCampaña,Column", 0),
    ("Informe de something something\nCampana,Column", 0),
]


@given(data=data())
@pytest.mark.parametrize("preamble", BRANDWATCH_PREAMBLES)
def test_brandwatch(preamble, data):
    preamble, skip_n_exp = preamble
    csv = preamble + "\n" + data.draw(csv_strat(header=3))
    skip_n_det = Brandwatch().detect(io.StringIO(csv))
    assert equal(skip_n_exp, skip_n_det)


@given(data=data())
@pytest.mark.parametrize("preamble", FIELDLESS_PREAMBLES)
def test_fieldless(preamble, data):
    preamble, skip_n_exp = preamble
    csv = preamble + "\n" + data.draw(csv_strat(header=2))
    skip_n_det = Fieldless().detect(io.StringIO(csv))
    assert equal(skip_n_exp, skip_n_det)


@given(data=data())
@pytest.mark.parametrize("preamble", GOOGLEADS_PREAMBLES)
def test_googleads(preamble, data):
    preamble, skip_n_exp = preamble
    csv = preamble + "\n" + data.draw(csv_strat(header=2))
    skip_n_det = GoogleAds().detect(io.StringIO(csv))
    assert equal(skip_n_exp, skip_n_det)


@given(data=data())
@pytest.mark.parametrize("preamble", BRANDWATCH_PREAMBLES + FIELDLESS_PREAMBLES)
def test_preambles(preamble, data):
    preamble, skip_n_exp = preamble
    csv = preamble + "\n" + data.draw(csv_strat(header=3))
    skip_n_det = Preambles.detect(io.StringIO(csv))
    assert equal(skip_n_exp, skip_n_det)
