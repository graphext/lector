"""Subpackage for smart parsing of CSV files.

Helps deteting encoding, preambles (initial junk to skip), CSV dialects etc.
"""
from .abc import EmptyFileError, Format, Reader
from .arrow import ArrowReader
from .dialects import Dialect, PySniffer
from .encodings import Chardet
from .preambles import Preambles

__all__ = [
    "ArrowReader",
    "Chardet",
    "Dialect",
    "EmptyFileError",
    "Format",
    "Preambles",
    "PySniffer",
    "Reader",
]
