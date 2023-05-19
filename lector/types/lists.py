"""List parsing and casting.

Currently NOT supported in CSV strings:

- floats with comma as the decimal delimiter (must be the period character)
- floats with thousands separator
"""
from __future__ import annotations

from collections.abc import Iterable
from contextlib import suppress
from csv import reader as csvreader
from dataclasses import dataclass

import msgspec
import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array, DataType

from ..log import LOG
from ..utils import ensure_type, min_max, proportion_trueish, smallest_int_type
from .abc import Conversion, Converter, Registry
from .regex import RE_LIST_CLEAN, RE_LIST_LIKE
from .strings import proportion_url

LIST_TYPES: tuple[str] = (pa.int64(), pa.float64(), pa.timestamp(unit="ms"))

JSON_DECODE = msgspec.json.Decoder(type=list).decode

SAFE_CSV_PARSING = False


def parse_csvs(strings: Iterable[str], safe=SAFE_CSV_PARSING, **kwds) -> Iterable[list]:
    """Parse a list of strings as CSV, to separate it into individual fields.

    The non-safe option uses python's built-in reader. But it either raises on invalid
    rows, or silently returns fewer parsed rows than original rows, depending on the
    "strict" parameter. The safe option will always return the expected number of rows,
    with values being None where a string couldn't be parsed.
    """
    if safe:
        for s in strings:
            try:
                yield next(csvreader([s], **kwds))
            except Exception:
                yield None
    else:
        yield from csvreader(strings, **kwds)


def parse_lists_csv(arr: Array, **kwds) -> Array:
    """Parse strings as lines of CSV, to separate it into individual fields.

    Respects the separator being escaped when enclosed in (double) quotes etc.
    """
    content = pac.replace_substring_regex(arr, pattern=RE_LIST_CLEAN, replacement="")
    strings = (s.as_py() if s.is_valid else "" for s in content)
    lists = parse_csvs(strings, **kwds)
    lists = ([elem.strip("' ") for elem in l] if l is not None else l for l in lists)
    result = pa.array(lists)
    result = pac.if_else(arr.is_null(), pa.NA, result)  # Restore original nulls
    return result


def parse_json(s: str):
    """Parse a single string as json."""
    l = JSON_DECODE(s)

    if l and any(isinstance(x, (list, dict)) for x in l):
        l = [str(x) for x in l]

    return l


def parse_lists_json(arr: Array) -> Array:
    """Parse strings as lists using the significantly faster msgspec."""
    parsed = (parse_json(s.as_py()) if s.is_valid else None for s in arr)
    return pa.array(parsed)


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

        if arr.type == type:
            return arr

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
    quote_char: str = '"',
    delimiter: str = ",",
) -> Array | None:
    """Parse strings into list, optionally with (inferrable) element type."""
    if proportion_listlike(arr.drop_null()) < threshold:
        return None

    try:
        result = parse_lists_json(arr)
        LOG.debug("[List] Was able to fast-parse as json")
    except Exception:
        try:
            result = parse_lists_csv(
                arr, skipinitialspace=True, quotechar=quote_char, delimiter=delimiter
            )
        except Exception as exc:
            LOG.error(f"Cannot parse lists as CSV: {exc}")
            return None

    if type is not None:
        return result.cast(pa.list_(ensure_type(type)))

    return maybe_cast_lists(result, types=LIST_TYPES) or result


@dataclass
@Registry.register
class List(Converter):
    type: str | DataType | None = None
    infer_urls: bool = True
    threshold_urls: float = 1.0
    quote_char: str = '"'
    delimiter: str = ","

    def convert(self, array: Array) -> Conversion | None:
        result = None

        if pat.is_string(array.type):
            result = maybe_parse_lists(
                array,
                type=self.type,
                threshold=self.threshold,
                quote_char=self.quote_char,
                delimiter=self.delimiter,
            )
        elif pat.is_list(array.type):
            result = array

        if result is None:
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
            semantic = f"list[number[{vtype}]]"
        else:
            if str(vtype) == "float":
                # pyarrow's "float" means float32, but pandas would interpret it as float64
                vtype = "float32"

            semantic = f"list[number[{vtype}]]"

        return Conversion(result, meta={"semantic": semantic})
