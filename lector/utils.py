"""Common helpers to work with pyarrow objects."""

from __future__ import annotations

import json
from collections import namedtuple
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from functools import singledispatch
from time import perf_counter
from typing import Any, Union

import pyarrow as pa
from pyarrow import (
    Array,
    ChunkedArray,
    DataType,
    Schema,
    Table,
    type_for_alias,  # noqa: F401
)
from pyarrow import compute as pac
from pyarrow import types as pat
from pyarrow.lib import ensure_type  # noqa: F401

try:
    import pandas as pd

    PANDAS_INSTALLED = True
except Exception:
    PANDAS_INSTALLED = False


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


@contextmanager
def reset_buffer(buffer):
    """Caches and resets buffer position."""
    cursor = buffer.tell()
    yield
    buffer.seek(cursor)


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

    if pat.is_integer(type) and arr.null_count > 0:
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

    return valcnt.take(order[:top_n])


def map_values(arr: Array, map: dict, unknown: str = "keep") -> Array:
    """Slow value mapping in pure Python while Arrow doesn't have a native compute function.

    For now assumes type can be left unchanged.
    """
    values = arr.to_pylist()

    if unknown == "keep":
        values = [map.get(val, val) for val in values]
    else:
        values = [map.get(val) for val in values]

    return pa.array(values, type=arr.type)


def categories(array: Array | ChunkedArray) -> Array:
    """Returns an array containing categories in input array of dictionary type."""

    if not pat.is_dictionary(array.type):
        raise TypeError("Must have an array with dictionary type!")

    if isinstance(array, ChunkedArray):
        array = array.unify_dictionaries()
        return array.chunk(0).dictionary

    return array.dictionary


def is_stringy(type: DataType) -> bool:
    """Check if array is stringy (string or dictionary of strings)."""
    if pat.is_string(type):
        return True

    return pat.is_dictionary(type) and pat.is_string(type.value_type)


def with_flatten(arr: Array, func: Callable):
    """Apply a compute function to all elements of flattened (and restored) lists."""
    isna = pac.is_null(arr)
    flat = pac.list_flatten(arr)
    transformed = func(flat)
    nested = pa.ListArray.from_arrays(arr.offsets, transformed)
    return pac.if_else(isna, None, nested)


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


def maybe_load_json(s: str) -> Any:
    """Try to load a string as json, returning the original string if it fails."""
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return s


def decode_metadata(d: dict):
    """Decode Arrow metadata to dict."""
    return {k.decode("utf-8"): maybe_load_json(v.decode("utf-8")) for k, v in d.items()}


class Timer:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.end = perf_counter()
        self.elapsed = self.end - self.start


if PANDAS_INSTALLED:
    # Arrow currently doesn't have any way to map its integer types to pandas
    # extension dtypes depending on whether a columns has missing values or not

    @singledispatch
    def to_pandas(array: Array):
        """Proper conversion allowing pandas extension types."""

        atype = array.type

        if pat.is_string(atype):
            return array.to_pandas().astype("string")

        if pat.is_boolean(atype):
            return array.to_pandas().astype("boolean")

        if pat.is_integer(atype) and array.null_count > 0:
            dtype_name = str(atype).replace("i", "I").replace("u", "U")
            return array.to_pandas(integer_object_nulls=True).astype(dtype=dtype_name)

        return array.to_pandas()

    @to_pandas.register
    def _(table: Table):
        columns = [to_pandas(array) for array in table]
        df = pd.concat(columns, axis=1)
        df.columns = table.column_names
        return df


def uniquify(items: Sequence[str]) -> Iterator[str]:
    """Add suffixes to inputs strings if necessary to ensure is item is unique."""
    seen = set()

    for item in items:
        newitem = item

        suffix = 0
        while newitem in seen:
            suffix += 1
            newitem = f"{item}_{suffix}"

        seen.add(newitem)
        yield newitem
