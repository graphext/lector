"""List parsing and casting.

Currently NOT supported in CSV strings:

- floats with comma as the decimal delimiter (must be the period character)
- floats with thousands separator
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import suppress
from csv import reader as csvreader
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array, DataType

from ..log import LOG
from ..utils import ensure_type, min_max, proportion_trueish, smallest_int_type
from .abc import Conversion, Converter, Registry
from .regex import RE_LIST_LIKE, RE_LIST_PARENS
from .strings import proportion_url

LIST_TYPES: tuple[str] = (pa.int64(), pa.float64(), pa.timestamp(unit="ms"))


def parse_lists(
    strings: Iterator[str],
    clean_elems: bool = False,
    skipinitialspace: bool = True,
    **kwds,
) -> Iterator[list[str]]:
    """Parse strings as lines of CSV, to separate it into individual fields.

    Respects the separator being escaped when enclosed in quotes etc.
    """
    reader = csvreader(strings, skipinitialspace=skipinitialspace, **kwds)
    if not clean_elems:
        yield from reader
    else:
        for parsed in reader:
            yield [elem.strip("' ") for elem in parsed]


def proportion_listlike(arr: Array) -> float:
    """Calculate proportion of non-null strings that could be lists."""
    valid = arr.drop_null()
    is_list = pac.match_substring_regex(valid, RE_LIST_LIKE)
    return proportion_trueish(is_list)


def maybe_cast_lists(
    arr: Array,
    types: list[DataType | str] = LIST_TYPES,
    downcast: bool = True,
) -> Array | None:
    """Cast lists (of strings) to first valid type, if any."""

    for type in types:
        type = ensure_type(type)

        with suppress(Exception):
            result = pac.cast(arr, pa.list_(type))

            if type == "int64" and downcast:
                vmin, vmax = min_max(pac.list_flatten(result))
                itype = smallest_int_type(vmin, vmax)

                if itype is not None:
                    try:
                        itype = ensure_type(itype)
                        result = pac.cast(result, pa.list_(itype))
                    except Exception as exc:
                        LOG.error(exc)
                        LOG.error("Will not downcast lists of int64.")

            return result

    return None


def maybe_parse_lists(
    arr: Array,
    type: str | DataType | None = None,
    threshold: float = 1.0,
    allow_empty: bool = True,
) -> Array | None:
    """Parse strings into list, optionally with (inferrable) element type."""
    if proportion_listlike(arr.drop_null()) < threshold:
        return None

    content = pac.replace_substring_regex(arr, pattern=RE_LIST_PARENS, replacement="")
    parsed = parse_lists((s.as_py() if s.is_valid else "" for s in content), clean_elems=True)
    result = pa.array(parsed, size=len(content), type=pa.list_(pa.string()))
    result = pac.if_else(arr.is_null(), pa.NA, result)  # Keep original nulls

    if allow_empty:
        was_empty = pac.equal(arr, "[]")
        result = pac.if_else(was_empty, pa.scalar([], type=pa.list_(pa.string())), result)

    if type is not None:
        return result.cast(pa.list_(ensure_type(type)))

    return maybe_cast_lists(result, types=LIST_TYPES) or result


@dataclass
@Registry.register
class List(Converter):
    type: str | DataType | None = None
    infer_urls: bool = True
    threshold_urls: float = 1.0

    def convert(self, array: Array) -> Conversion | None:
        if not pat.is_string(array.type):
            return None

        result = maybe_parse_lists(array, self.type, self.threshold)

        if not result:
            return None

        vtype = result.type.value_type

        if pat.is_string(vtype):
            if self.infer_urls and proportion_url(pac.list_flatten(result)) >= self.threshold_urls:
                semantic = "list[url]"
            else:
                semantic = "list[category]"
        elif pat.is_timestamp(vtype):
            semantic = "list[date]"
        elif pat.is_integer(vtype):
            semantic = "list[number[int64]]"
        else:
            semantic = "list[number[float64]]"

        return Conversion(result, meta={"semantic": semantic})
