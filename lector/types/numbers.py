"""Helpers for parsing and downcasting numeric data.

Note: Arrow uses Google's RE2 to implement regex functionality:
https://github.com/google/re2/wiki/Syntax

"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from enum import Enum

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
    smallest_int_type,
)
from .abc import Conversion, Converter, Registry
from .regex import RE_IS_FLOAT, RE_IS_INT

DECIMAL_SUPPORT_MIN = 0.2  # 20%
DECIMAL_CONFIDENCE_MIN = 1.5  # 150%


class DecimalMode(str, Enum):
    INFER = "INFER"
    COMPARE = "COMPARE"


def clean_float_pattern(thousands: str = ",") -> str:
    """Removes characters in number strings that Arrow cannot parse."""
    if thousands == ",":
        # Match a "+" at the beginning and commas anywhere
        return r"^\+|,"

    # Match a "+" at the beginning and a period anywhere
    return r"^\+|\."


def decimal_delimiter(  # noqa: PLR0911, PLR0912
    s: str,
    n_chars_max: int = 20,
) -> str | None:
    """Infer decimal delimiter from string representation s of an input number.

    Returns None if not unambiguously inferrable.
    """
    n_commas = n_dots = n_delims = 0
    first_comma_idx = first_dot_idx = None
    n = len(s)

    for i, c in enumerate(s):
        if i > n_chars_max and n_delims == 0:  # noqa: PLR2004
            return None  # Early out for long strings that are unlikely to represent numbers

        if c in ".,":
            if i == 0 or (i == 1 and s[0] == "0"):
                return c  # ".123" or "0.123": can only be decimal

            if i >= 4 and n_delims == 0:  # noqa: PLR2004
                return c  # First delim at 5th position: cannot be thousands (1234.00)

            if i + 3 >= n:
                return c  # Less than 3 characters after delim: cannot be thousands (1.12)

            n_delims += 1

            if c == ".":
                n_dots += 1
                if first_dot_idx is None:
                    first_dot_idx = i
            else:
                n_commas += 1
                if first_comma_idx is None:
                    first_comma_idx = i

    if n_dots == 1 and n_commas == 0:
        return "."
    if n_dots > 0 and n_commas > 0:
        return "." if first_comma_idx < first_dot_idx else ","
    if n_commas == 1 and n_dots == 0:
        return ","
    if n_commas > 1:
        return "."
    if n_dots > 1:
        return ","

    return None


def infer_decimal_delimiter(arr: Array) -> str | None:
    """Get most frequent decimal delimiter in array.

    If most frequent delimiter doesn't occur in sufficient proportion (support),
    or not significantly more often than other delimiters (confidence), returns
    None.
    """
    n = len(arr)
    counts = Counter(decimal_delimiter(s.as_py()) for s in arr)
    counts.update({".": 0, ",": 0})
    ranked = [d for d in counts.most_common(3) if d[0]]

    if all(delim[1] == 0 for delim in ranked):
        return None

    if ranked[1][1] > 0:
        # If ambiguous

        if (ranked[0][1] / n) < DECIMAL_SUPPORT_MIN:
            # Most frequent delimiter should occur in at least 30% of rows
            return None

        if (ranked[0][1] / ranked[1][1]) < DECIMAL_CONFIDENCE_MIN:
            # Most frequent delimiter should occur at least 50% more often than next delimiter
            return None

    return ranked[0][0]


def clean_float_strings(arr: Array, decimal: str) -> tuple[Array, Array, float]:
    """Prepare an array of strings so that Arrow can cast the result to floats.

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
    """
    thousands = "," if decimal == "." else "."
    pattern = clean_float_pattern(thousands)
    clean = pac.replace_substring_regex(arr, pattern=pattern, replacement="")
    if decimal == ",":
        clean = pac.replace_substring(clean, pattern=",", replacement=".", max_replacements=1)

    # Arrow doesn't recognize upper case exponential ("1.03481E-11")
    clean = pac.utf8_lower(clean)
    is_float = pac.match_substring_regex(clean, pattern=RE_IS_FLOAT)

    if is_float.null_count == len(is_float):
        prop_valid = 0.0
    else:
        prop_valid = pac.sum(is_float).as_py() / (len(arr) - arr.null_count)

    return clean, is_float, prop_valid


def maybe_parse_ints(
    arr: Array,
    threshold: float = 1.0,
    allow_unsigned: bool = False,
) -> Array | None:
    """Use regex to extract castable ints.

    Arrow's internal casting from string to int doesn't allow for an
    initial positive sign character, so we have to handle that separately.
    """
    is_int = pac.match_substring_regex(arr, pattern=RE_IS_INT)
    if is_int.null_count == len(is_int):
        return None

    valid_prop = pac.sum(is_int).as_py() / (len(arr) - arr.null_count)
    if valid_prop < threshold:
        return None

    clean = pac.if_else(is_int, arr, None)
    clean = pac.replace_substring_regex(clean, r"^\+", "")

    try:
        return pac.cast(clean, pa.int64())
    except Exception:
        if allow_unsigned:
            try:
                return pac.cast(clean, pa.uint64())
            except Exception as exc:
                LOG.error(exc)

    return None


def maybe_parse_floats(
    arr: Array,
    threshold: float = 0.5,
    decimal: str | DecimalMode = DecimalMode.INFER,
) -> Array | None:
    """Parse valid string representations of floating point numbers."""
    if decimal == DecimalMode.INFER:
        decimal = infer_decimal_delimiter(arr.drop_null())
        if decimal is None:
            return None

    if isinstance(decimal, str) and decimal in ".,":
        clean, is_float, prop_valid = clean_float_strings(arr, decimal=decimal)
    elif decimal == DecimalMode.COMPARE:
        result_dot = clean_float_strings(arr, decimal=".")
        result_comma = clean_float_strings(arr, decimal=",")
        if result_dot[2] >= result_comma[2]:
            clean, is_float, prop_valid = result_dot
        else:
            clean, is_float, prop_valid = result_comma
    else:
        raise ValueError(f"Must have decimal char or one of ['infer', 'compare']! Got '{decimal}'.")

    if prop_valid < threshold:
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
    if (vmin is None) or (vmax is None):
        return None

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
            return Conversion(result) if result is not None else Conversion(array)

        return None


@dataclass
@Registry.register
class Number(Converter):
    """Attempts to parse strings into floats or ints followed by downcasting."""

    decimal: str | DecimalMode = DecimalMode.INFER
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

            if converted is not None:
                downcast = Downcast().convert(converted)
                converted = downcast if downcast is not None else Conversion(converted)
        else:
            converted = Downcast().convert(array)

        if converted is None:
            return None

        if (
            pat.is_integer(converted.result.type)
            and self.max_int is not None
            and (pac.max(converted.result).as_py() or 0) > self.max_int
        ):
            return None

        converted.meta = {"semantic": f"number[{dtype_name(converted.result)}]"}
        return converted
