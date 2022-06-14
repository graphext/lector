"""Subpackage for smart parsing of CSV files.

Helps deteting encoding, preambles (initial junk to skip), CSV dialects etc.
"""
from .abc import EmptyFileError, Format, Reader
from .arrow import ArrowReader
from .dialects import Dialect, PySniffer
from .pandas import PandasReader
from .preambles import Preambles

__all__ = [
    "ArrowReader",
    "EmptyFileError",
    "Dialect",
    "Format",
    "PandasReader",
    "Preambles",
    "Reader",
]
