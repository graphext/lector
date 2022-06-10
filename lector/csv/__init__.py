"""Subpackage for smart parsing of CSV files.

Helps deteting encoding, preambles (initial junk to skip), CSV dialects etc.
"""
from .abc import EmptyFileError, Format, Reader
from .arrow import ArrowReader
from .dialects import Dialect
from .pandas import PandasReader

__all__ = ["ArrowReader", "EmptyFileError", "Dialect", "Format", "PandasReader", "Reader"]
