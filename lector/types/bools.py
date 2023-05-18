"""Classes for converting arrays to the boolean type."""
from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass

import pyarrow as pa
from pyarrow import Array

from ..utils import is_stringy
from .abc import Conversion, Converter, Registry


@dataclass
@Registry.register
class Boolean(Converter):
    """Converts stringy booleans ("true" / "False"), and ints (0/1) to the boolean type."""

    def convert(self, array: Array) -> Conversion | None:
        if not is_stringy(array.type) or array.null_count == len(array):
            return None

        meta = {"semantic": "boolean"}

        with suppress(pa.ArrowInvalid):
            converted = array.cast(pa.bool_())

            n = len(array)
            valid_before = n - array.null_count
            valid_after = n - converted.null_count
            proportion_valid = valid_after / valid_before

            if proportion_valid >= self.threshold:
                return Conversion(converted, meta=meta)

        return None
