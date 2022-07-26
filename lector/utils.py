"""Common helpers to work with pyarrow objects."""
from __future__ import annotations

import json
from collections import namedtuple
from time import perf_counter
from typing import Union

import pyarrow as pa
from pyarrow import type_for_alias  # noqa: F401
from pyarrow import Array, ChunkedArray, DataType, Schema
from pyarrow import compute as pac
from pyarrow import types as pat
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
    # Would expect this to happen automatically, but not the case
    # (at least when Arrow reads CSV with types="string")
    "",
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


def dtype_name(arr: Array):
    """Return a pandas-compatible type name including extension types where possible."""
    type = arr.type
    name = str(type)

    if pat.is_integer(type):
        if arr.null_count > 0:
            name = name.replace("i", "I").replace("u", "U")

    return name


def min_max(arr: Array, skip_nulls: bool = True) -> tuple[Number, Number]:
    """Wrapper to get minimum and maximum in arrow array as python tuple."""
    mm = pac.min_max(arr, skip_nulls=skip_nulls).as_py()
    return mm["min"], mm["max"]


def proportion_valid(arr: Array) -> float:
    """Proportion of non-null values in array."""
    size = len(arr)
    return (size - arr.null_count) / size


def proportion_unique(arr: Array) -> float:
    """Proportion of non-null values that are unique in array."""
    n_valid = len(arr) - arr.null_count

    if n_valid == 0:
        return 0

    n_unique = pac.count_distinct(arr, mode="only_valid").as_py()
    return n_unique / n_valid


def proportion_trueish(arr: Array) -> float:

    if len(arr) == 0:
        # Still means we had no trueish values
        return 0

    n_trueish = pac.sum(arr).as_py() or 0  # may return None otherwise, which we consider falsish
    return n_trueish / len(arr)


def proportion_equal(arr1: Array, arr2: Array, ignore_nulls=True) -> float:
    """Proportion of equal values, optionally ignoring nulls (which otherwise compare falsish."""
    equal = pac.equal(arr1, arr2)
    if ignore_nulls:
        equal = equal.drop_null()

    return proportion_trueish(equal)


def empty_to_null(arr: Array) -> Array:
    """Convert empty strings to null values."""
    is_empty = pac.equal(arr, "")
    return pac.if_else(is_empty, None, arr)


def sorted_value_counts(arr: Array, order: str = "descending", top_n: int | None = None) -> Array:
    """Arrow's built-in value count doesn't allow sorting."""
    valcnt = arr.value_counts()
    counts = valcnt.field("counts")
    order = pac.array_sort_indices(counts, order="descending")
    if top_n is None:
        return valcnt.take(order)
    else:
        return valcnt.take(order[:top_n])


def map_values(arr: Array, map: dict, unknown: str = "keep") -> Array:
    """Slow value mapping in pure Python while Arrow doesn't have a native compute function.

    For now assumes type can be left unchanged.
    """
    values = arr.to_pylist()

    if unknown == "keep":
        values = [map.get(val, val) for val in values]
    else:
        values = [map.get(val, None) for val in values]

    return pa.array(values, type=arr.type)


def categories(array: Array | ChunkedArray) -> Array:
    """Returns an array containing categories in input array of dictionary type."""

    if not pat.is_dictionary(array.type):
        raise TypeError("Must have an array with dictionary type!")

    if isinstance(array, ChunkedArray):
        array = array.unify_dictionaries()
        return array.chunk(0).dictionary
    else:
        return array.dictionary


def schema_diff(s1: Schema, s2: Schema) -> dict[str, tuple[DataType, DataType]]:
    """Check differences in schema's column types."""
    diff = {}

    for field in s1:
        other = s2.field(field.name)
        if field.type != other.type:
            diff[field.name] = (field.type, other.type)

    return diff


def encode_metadata(d: dict):
    """Json-byte-encode a dict, like Arrow expects its metadata."""
    return {k.encode("utf-8"): json.dumps(v).encode("utf-8") for k, v in d.items()}


def decode_metadata(d: dict):
    """Decode Arrow metadata to dict."""
    return {k.decode("utf-8"): json.loads(v.decode("utf-8")) for k, v in d.items()}


class Timer:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.end = perf_counter()
        self.elapsed = self.end - self.start


# def test_numeric_predicates():
#     """Check which number representations can be recognized/converted in Python/Arrow.

#     It seems trivial, but the nomenclature of string->numeric predicates, i.e. of identifying
#     strings as representing specific types of numbers is rather counter-intuitive:

#     - "decimal" doesn't refer to proper decimal numbers (i.e. including fractions), but to
#       pure and positive(!) integers only. In both Python and Arrow.
#     - "digit" in Python consists of decimals plus subscript and superscript characters.
#       But not in Arrow, where there doesn't seem to be a difference between decimal and digit.
#     - "numeric" in both Python and Arrow includes sub/superscripts as well as "vulgar"
#       fractions, special numeral characters etc.

#     In other words, none can be used to check for either conversion to integers nor float, as
#     none supports indication of sign (+/-), nor decimals.
#     """
#     ns = ["123", "1.23", "1,123.45", "²", "⅓"]
#     for n in ns:
#         print(n)
#         print("Python:", n.isdecimal(), n.isdigit(), n.isnumeric())
#         print("Arrow:", pac.utf8_is_decimal(n), pac.utf8_is_digit(n), pac.utf8_is_numeric(n))
#         try:
#             pa.scalar(n, pa.string()).cast(pa.float64())
#             print("Casteable to number(float).")
#         except Exception:
#             print("Cannot be cast to number!")
