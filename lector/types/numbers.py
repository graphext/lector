from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array

from ..utils import min_max, proportion_equal, smallest_int_type
from .abc import Conversion, Converter, Registry


def maybe_truncate_floats(arr: Array, threshold: float = 1.0) -> Array | None:
    """Float to int conversion if sufficient values are kept unchanged."""
    trunc = pac.trunc(arr)
    prop_truncable = proportion_equal(arr, trunc)

    if prop_truncable >= threshold:

        if pac.min(arr).as_py() >= 0:
            return pac.cast(trunc, pa.uint64())
        else:
            return pac.cast(trunc, pa.int64())

    return None


def maybe_downcast_ints(arr: Array) -> Array | None:
    """Convert to smallest applicable int type."""
    vmin, vmax = min_max(arr, skip_nulls=True)
    type = smallest_int_type(vmin, vmax)

    if type is not None:
        return pac.cast(arr, type)

    return None


# @dataclass
# class FloatToInt(Converter):
#     """Attempts to cast to int after truncating the original floats."""

#     def convert(self, array: Array) -> Converted | None:

#         if not pat.is_floating(array.type):
#             return None

#         trunc = pac.trunc(array)
#         prop_truncable = proportion_equal(array, trunc)

#         if prop_truncable >= self.threshold:

#             if pac.min(array).as_py() >= 0:
#                 result = pac.cast(trunc, pa.uint64())
#             else:
#                 result = pac.cast(trunc, pa.int64())

#             return Converted(result)

#         return None


# @dataclass
# class DowncastInt(Converter):
#     """Find the smallest int subtype that can hold all values."""

#     def convert(self, array: Array) -> Converted | None:

#         if not pat.is_integer(array.type):
#             return None

#         vmin, vmax = min_max(array, skip_nulls=True)
#         type = smallest_int_type(vmin, vmax)
#         if type is not None:
#             return Converted(pac.cast(array, type))

#         return None


@dataclass
@Registry.register
class Number(Converter):
    """Attempts to cast to int after truncating the original floats."""

    def convert(self, array: Array) -> Conversion | None:

        if pat.is_floating(array.type):
            array = maybe_truncate_floats(array, self.threshold)
            if array is None:
                return None

        if pat.is_integer(array.type):
            result = maybe_downcast_ints(array)
            return Conversion(result) if result else Conversion(array)

        return None
