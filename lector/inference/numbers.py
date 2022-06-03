from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array

from ..utils import min_max, proportion_equal, smallest_int_type


def maybe_truncate_floats(arr: Array, threshold: float = 1.0) -> Array:
    """Float to int conversion if sufficient values are kept unchanged."""
    trunc = pac.trunc(arr)
    prop_truncable = proportion_equal(arr, trunc)

    if prop_truncable >= threshold:

        if pac.min(arr).as_py() >= 0:
            return pac.cast(trunc, pa.uint64())
        else:
            return pac.cast(trunc, pa.int64())

    return arr


def maybe_downcast_ints(arr: Array) -> Array:
    """Convert to smallest applicable int type."""
    vmin, vmax = min_max(arr, skip_nulls=True)
    type = smallest_int_type(vmin, vmax)
    if type is not None:
        return pac.cast(arr, type)

    return arr


def autocast_numbers(arr: Array, int_cast_threshold: float = 1.0) -> Array:
    """Try to find most efficient numeric subtype for array."""

    if pat.is_floating(arr.type):
        arr = maybe_truncate_floats(arr, threshold=int_cast_threshold)

    if pat.is_integer(arr.type):
        arr = maybe_downcast_ints(arr)

    return arr
