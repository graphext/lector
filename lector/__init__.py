"""A package for fast parsing of messy CSV files and smart-ish type inference."""
from __future__ import annotations

from enum import Enum

from . import utils
from .csv import ArrowReader, Dialect, EmptyFileError, Format, Preambles
from .csv.abc import FileLike, PreambleRegistry
from .csv.dialects import DialectDetector
from .csv.encodings import EncodingDetector
from .log import CONSOLE, LOG, schema_view, table_view
from .types import Autocast, Cast, Converter, Registry
from .types.cast import CastStrategy


class Inference(str, Enum):

    Native = "Native"
    Auto = "Auto"
    Disable = "Disable"


def read_csv(
    fp: FileLike,
    encoding: str | EncodingDetector | None = None,
    dialect: dict | DialectDetector | None = None,
    preamble: int | PreambleRegistry | None = None,
    types: str | dict | Inference = Inference.Auto,
    strategy: CastStrategy | None = None,
    to_pandas: bool = False,
    log: bool = False,
):
    """Thin wrapper around class-based reader interface."""

    reader = ArrowReader(fp, encoding=encoding, dialect=dialect, preamble=preamble, log=log)

    dtypes = types
    if isinstance(types, Inference):
        dtypes = None if types == Inference.Native else "string"

    tbl = reader.read(types=dtypes)

    if types == Inference.Auto:
        strategy = strategy or Autocast(log=log)
        tbl = strategy.cast(tbl)

    if to_pandas:
        if utils.PANDAS_INSTALLED:
            return utils.to_pandas(tbl)
        else:
            raise ("It seems pandas isn't installed in this environment!")

    return tbl


__all__ = [
    "Autocast",
    "ArrowReader",
    "Cast",
    "CONSOLE",
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

__version__ = "0.2.14"
