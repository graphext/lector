"""Helpers for parsing and downcasting numeric data.

Note: Arrow uses Google's RE2 to implement regex functionality:
https://github.com/google/re2/wiki/Syntax

"""
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array

from ..log import LOG
from ..utils import (
    dtype_name,
    empty_to_null,
    min_max,
    proportion_equal,
    proportion_trueish,
    smallest_int_type,
)
from .abc import Conversion, Converter, Registry
from .regex import RE_INT, RE_IS_FLOAT


def clean_float_pattern(thousands: str = ",") -> str:
    """Removes characters in number strings that Arrow cannot parse."""
    if thousands == ",":
        # Match a "+" at the beginning and commas anywhere
        return r"^\+|,"

    # Match a "+" at the beginning and a period anywhere
    return r"^\+|\."


def maybe_parse_ints(
    arr: Array,
    threshold: float = 1.0,
    allow_unsigned: bool = False,
) -> Array | None:
    """Use regex to extract casteable ints.

    Arrow's internal casting from string to int doesn't allow for an
    initial sign character, so we have to handle that separately.

    Arrows regex returns a struct array containing a ``is_valid`` field
    and one field per capture group (the groups in the regex need to
    be named instead of just numbered, while the result can only be inspected
    using group indices, hmmm). Unfortunately, the captured fields
    will be empty strings rather than null, where no match was found.
    Only ``is_valid`` knows which rows matched.
    """
    parsed = pac.extract_regex(arr, RE_INT)
    is_int = parsed.is_valid()

    valid_prop = pac.sum(is_int).as_py() / (len(arr) - arr.null_count)
    if valid_prop < threshold:
        return None

    # Get named capture groups by index
    num = pac.struct_field(parsed, [1])
    num = pac.if_else(is_int, num, None)  # "" -> null
    is_negative = pac.equal(pac.struct_field(parsed, [0]), "-")

    try:
        num = pac.cast(num, pa.int64())
        sign = pac.if_else(is_negative, -1, 1)
        return pac.multiply_checked(num, sign)
    except Exception:
        if allow_unsigned:
            n_negative = pac.sum(is_negative).as_py()

            if n_negative == 0:
                try:
                    return pac.cast(num, pa.uint64())
                except Exception as exc:
                    LOG.error(exc)

    return None


def maybe_parse_floats(arr: Array, threshold: float = 0.5, decimal: str = ".") -> Array | None:
    """Parse valid string representations of floating point numbers.

    Arrow allows exponential syntax and omission of 0s before and after the decimal point,
    i.e. the following are all valid string representations of floating point numbers:
    "-1e10", "1e10", "1e-2", "1.2e3", "-1.2e3", "1." ".12", "-1.", "-.1".

    Arrow doesn't allow prefix of a positive sign indicator, nor thousands separator, i.e.
    the following are not(!) valid:
    "+1e10", "+1.", "+.1", "123,456.0"

    We hence remove occurrences of both the thousands character and the positive sign
    before extracting the floating point part of strings using regex.

    Also see following for more regex parsing options:
    https://stackoverflow.com/questions/12643009/regular-expression-for-floating-point-numbers

    Note, we don't parse as float if there isn't a single value with decimals. If this is
    the case they should be integers really, and if they haven't been parsed as ints before,
    that's because the values didn't fit into Arrow's largesy integer type, in which case it
    isn't safe to parse as float, which Arrow would otherwise do unsafely(!) and silently.

    TODO:

    - try to fold the empty string case into the regex directly to avoid another pass
      over the data

    """
    if pac.sum(pac.count_substring(arr, pattern=decimal)).as_py() == 0:
        return None

    thousands = "," if decimal == "." else "."
    pattern = clean_float_pattern(thousands)
    clean = pac.replace_substring_regex(arr, pattern=pattern, replacement="")
    clean = pac.utf8_lower(clean)  # Arrow doesn't recognize upper case exponential ("1.03481E-11")

    is_float = pac.match_substring_regex(clean, pattern=RE_IS_FLOAT)
    if proportion_trueish(is_float) < threshold:
        return None

    valid = pac.if_else(is_float, clean, None)  # non-floats -> null
    valid = empty_to_null(valid)

    try:
        return pac.cast(valid, pa.float64())
    except Exception as exc:
        LOG.error(exc)

    return None


def maybe_truncate_floats(arr: Array, threshold: float = 1.0) -> Array | None:
    """Float to int conversion if sufficient values are kept unchanged."""
    trunc = pac.trunc(arr)

    if proportion_equal(arr, trunc) < threshold:
        return None

    try:
        if pac.min(arr).as_py() >= 0:
            return pac.cast(trunc, pa.uint64())

        return pac.cast(trunc, pa.int64())
    except pa.ArrowInvalid as exc:
        LOG.error("Failed to convert floats to ints: " + str(exc))
        return None


def maybe_downcast_ints(arr: Array) -> Array | None:
    """Convert to smallest applicable int type."""
    vmin, vmax = min_max(arr, skip_nulls=True)
    type = smallest_int_type(vmin, vmax)

    if type is not None:
        return pac.cast(arr, type)

    return None


@dataclass
@Registry.register
class Downcast(Converter):
    """Attempts truncation of floats to ints and then downcasting of ints."""

    def convert(self, array: Array) -> Conversion | None:
        if pat.is_floating(array.type):
            array = maybe_truncate_floats(array, self.threshold)
            if array is None:
                return None

        if pat.is_integer(array.type):
            result = maybe_downcast_ints(array)
            return Conversion(result) if result else Conversion(array)

        return None


@dataclass
@Registry.register
class Number(Converter):
    """Attempts to parse strings into floats or ints followed by downcasting."""

    decimal: str = "."
    allow_unsigned_int: bool = True
    max_int: int | None = None

    def convert(self, array: Array) -> Conversion | None:
        if pat.is_string(array.type):
            converted = maybe_parse_ints(
                array,
                threshold=self.threshold,
                allow_unsigned=self.allow_unsigned_int,
            )

            if converted is None:
                converted = maybe_parse_floats(
                    array,
                    threshold=self.threshold,
                    decimal=self.decimal,
                )

            if converted:
                downcast = Downcast().convert(converted)
                converted = downcast if downcast is not None else Conversion(converted)
        else:
            converted = Downcast().convert(array)

        if converted is None:
            return None

        if (
            pat.is_integer(converted.result.type)
            and self.max_int is not None
            and pac.max(converted.result).as_py() > self.max_int
        ):
            return None

        converted.meta = {"semantic": f"number[{dtype_name(converted.result)}]"}
        return converted
