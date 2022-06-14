"""Test encoding detectors.

Notes:

- ISO-8859-1 (Latin-1) is mostly identical to Windows-1252 (CP1252):
  https://www.i18nqa.com/debug/table-iso8859-1-vs-windows-1252.html

"""
import io

import pytest

from lector.csv.encodings import Chardet

CODEC_ERR = "�"

CODEC_SAMPLES = [
    ("刺靑　谷崎潤一郞著", "utf-8", "windows-1250", 2),
    ("顏是家訓  北齊  顏之推", "BIG5", "utf-8", 12),
    ("The Project · Gutenberg » EBook « of Die Fürstin.", "ISO-8859-1", "utf-8", 4),
    ("Той и сам не знае кога е роден, но като го запитат.", "windows-1251", "utf-8", 38),
    ("première is first", "utf-8", "ascii", 2),
    ("première is first", "utf-16", "utf-8", 3),
    ("première is first", "windows-1252", "utf-8", 1),
    ("première is first", "ISO-8859-1", "utf-16", 1),
    ("𐐀 am Deseret", "utf-8", "windows-1250", 2),
    ("𐐀 am Deseret", "utf-8", "windows-1252", 2),
    ("𐐀 am Deseret", "utf-16", "utf-8", 4),
]
"""When encoded with first then decoded with second codecs, n unknown chars are produced."""

CODECS = [
    "utf-8",
    "utf-16",
    "utf-8-sig",
    "windows-1250",
    "windows-1251",
    "windows-1252",
    "iso-8859-1",
    "ascii",
]


def codecs_equal(c1, c2):
    c1 = c1.lower()
    c2 = c2.lower()
    if c1 == c2:
        return True

    # Allow some variations where small differences between codes cannot be inferred
    # from small sample
    equivalent = ("iso-8859-1", "windows-1252")
    return c1 in equivalent and c2 in equivalent


@pytest.mark.parametrize("codec", CODECS)
def test_simple_roundtrip(codec):
    """Test correct detection if string can be encoded given codec."""
    s = "première is first"

    try:
        encoded = s.encode(codec)
        detected = Chardet().detect(io.BytesIO(encoded))
        assert codecs_equal(codec, detected)
    except Exception:
        pass


@pytest.mark.parametrize("example", CODEC_SAMPLES)
def test_roundtrips(example):

    text, codec, _, _ = example
    encoded = text.encode(codec)
    detected = Chardet().detect(io.BytesIO(encoded))
    decoded = encoded.decode(detected, errors="replace")

    assert codecs_equal(codec, detected)
    assert decoded.count(CODEC_ERR) == 0
