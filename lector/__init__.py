"""A package for fast parsing of messy CSV files and smart-ish type inference."""
from .csv import ArrowReader, Dialect, EmptyFileError, Format, PandasReader, Preambles
from .log import LOG, schema_view, table_view
from .types import Autocast

__all__ = [
    "Autocast",
    "ArrowReader",
    "EmptyFileError",
    "Dialect",
    "Format",
    "LOG",
    "PandasReader",
    "Preambles",
    "schema_view",
    "table_view",
]
