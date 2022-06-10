"""A package for fast parsing of messy CSV files and smart-ish type inference."""
from .csv import ArrowReader, Dialect, EmptyFileError, Format, PandasReader
from .types import Autocast

__all__ = [
    "Autocast",
    "ArrowReader",
    "EmptyFileError",
    "Dialect",
    "Format",
    "PandasReader",
]
