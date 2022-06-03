from __future__ import annotations

from collections import namedtuple
from typing import Union

from pyarrow import type_for_alias  # noqa: F401
from pyarrow import Array
from pyarrow import compute as pac
from pyarrow.lib import ensure_type  # noqa: F401

Number = Union[int, float]

Limit = namedtuple("Limit", "min,max")

INT_LIMITS: dict[str, Limit] = {
    "int8": Limit(-128, 127),
    "int16": Limit(-32_768, 32_767),
    "int32": Limit(-2_147_483_648, 2_147_483_647),
    "int64": Limit(-9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
    "uint8": Limit(0, 255),
    "uint16": Limit(0, 65_535),
    "uint32": Limit(0, 4_294_967_295),
    "uint64": Limit(0, 18_446_744_073_709_551_615),
}
"""Minimum and maximum for each integer subtype."""

MISSING_STRINGS: set[str] = {
    "#N/A",
    "#N/A N/A",
    "#NA",
    "-1.#IND",
    "-1.#INF",
    "-1.#QNAN",
    "1.#IND",
    "1.#INF",
    "1.#INF000000",
    "1.#QNAN",
    "-NaN",
    "-nan",
    "<NA>",
    "N/A",
    "n/a",
    "NA",
    "NAN",
    "NaN",
    "nan",
    "NULL",
    "Null",
    "null",
}
"""Extension of pandas and arrow default missing values."""


def smallest_int_type(vmin: Number, vmax: Number) -> str | None:
    """Find the smallest int type able to hold vmin and vmax."""

    if vmin >= 0:
        types = ["uint8", "uint16", "uint32"]
    else:
        types = ["int8", "int16", "int32"]

    for type in types:
        limits = INT_LIMITS[type]
        if vmin >= limits.min and vmax <= limits.max:
            return type

    return None


def min_max(arr: Array, skip_nulls: bool = True) -> tuple[Number, Number]:
    """Wrapper to get minimum and maximum in arrow array as python tuple."""
    mm = pac.min_max(arr, skip_nulls=skip_nulls).as_py()
    return mm["min"], mm["max"]


def proportion_unique(arr: Array) -> float:
    """Proportion of non-null values that are unique in array."""
    n_unique = pac.count_distinct(arr, mode="only_valid").as_py()
    n_valid = len(arr) - arr.null_count
    return n_unique / n_valid


def proportion_equal(arr1: Array, arr2: Array, ignore_nulls=True) -> float:
    """Proportion of equal values, optionally ignoring nulls (which otherwise compare falsish."""
    equal = pac.equal(arr1, arr2)
    if ignore_nulls:
        equal = equal.drop_null()

    n_equal = pac.sum(equal).as_py()
    return n_equal / len(equal)
