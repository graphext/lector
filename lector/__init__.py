"""A package for fast parsing of messy CSV files and smart-ish type inference."""
from .csv import ArrowReader, Dialect, EmptyFileError, Format, Preambles
from .log import LOG, schema_view, table_view
from .types import Autocast, Cast, Converter, Registry

__all__ = [
    "Autocast",
    "ArrowReader",
    "Cast",
    "Converter",
    "EmptyFileError",
    "Dialect",
    "Format",
    "LOG",
    "Preambles",
    "Registry",
    "schema_view",
    "table_view",
]

__version__ = "0.2.9"
