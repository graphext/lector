from __future__ import annotations

from dataclasses import dataclass

import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array, TimestampScalar

from ..utils import proportion_trueish, proportion_valid
from .abc import Conversion, Converter, Registry

RE_TRAILING_DECIMALS: str = r"\.(\d+)$"

TIMESTAMP_FORMATS: list[str] = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%I:%M:%S %p",
    "%Y-%m-%dT%I:%M %p",
    "%Y-%m-%d%n%H:%M:%S",
    "%Y-%m-%d%n%I:%M:%S %p",
    "%a %b %d %H:%M:%S %Y",
    "%a %b %d %I:%M:%S %p %Y",
    "%a %d %b %H:%M:%S %Y",
    "%a %d %b %I:%M:%S %p %Y",
    "%a, %b %d %H:%M:%S %Y",
    "%a, %b %d %I:%M:%S %p %Y",
    "%a, %d %b %H:%M:%S %Y",
    "%a, %d %b %I:%M:%S %p %Y",
    "%a %d %b %Y %H:%M:%S",
    "%a %d %b %Y %I:%M:%S %p",
    "%a, %d %b %Y %H:%M:%S",
    "%a, %d %b %Y %I:%M:%S %p",
]

DATE_FORMATS: list[str] = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%a %d %b %Y",
    "%a, %d %b %Y",
]

ISO_FORMAT: str = "ISO8601()"


def timestamp_formats(tz: bool = True) -> list[str]:
    formats = TIMESTAMP_FORMATS
    if tz:
        with_tz = lambda fmt: (fmt, fmt + " %z", fmt + " Z", fmt + " UTC")
        formats = [ext for fmt in formats for ext in with_tz(fmt)]
    formats.extend(DATE_FORMATS)
    return [ISO_FORMAT] + formats


ALL_FORMATS: list[str] = timestamp_formats()


def proportion_trailing_decimals(arr: Array) -> float:
    """Proportion of non-null dates in arr having fractional seconds."""
    valid = arr.drop_null()
    has_frac = pac.match_substring_regex(valid, RE_TRAILING_DECIMALS)
    return proportion_trueish((has_frac))


def find_format(ts: TimestampScalar) -> str | None:
    """Try to find the first format that can parse given date."""
    if pac.is_null(ts).as_py():
        return None

    for fmt in ALL_FORMATS:
        try:
            pac.strptime(ts, format=fmt, unit="s")
            return fmt
        except Exception:
            continue

    return None


def maybe_parse_known_timestamps(
    arr: Array,
    format: str,
    unit: str = "ms",
    threshold: float = 1.0,
) -> Array | None:
    """Helper for parsing with known format and no fractional seconds."""

    if threshold == 1.0:
        try:
            return pac.strptime(arr, format=format, unit=unit)
        except Exception:
            return None

    result = pac.strptime(arr, format=format, unit=unit, error_is_null=True)
    return result if proportion_valid(result) >= threshold else None


def maybe_parse_timestamps(
    arr: Array,
    format: str | None = None,
    unit: str = "ms",
    threshold: float = 1.0,
    return_format: bool = False,
) -> Array | None:
    """Parse lists of strings as dates with format inference."""

    if proportion_trailing_decimals(arr) > 0.1:
        split = pac.split_pattern(arr, ".", max_splits=1, reverse=True)
        arr = pac.list_element(split, 0)

    if format is None:
        formats = ALL_FORMATS
        valid = arr.drop_null()

        if len(valid) > 0:
            first_date = valid[0]
            first_format = find_format(first_date)
            if first_format is not None:
                print(f"First date '{first_date}' matches {first_format}")
                formats = ALL_FORMATS.copy()
                formats.remove(first_format)
                formats.insert(0, first_format)

    else:
        formats = [format]

    for fmt in formats:
        result = maybe_parse_known_timestamps(arr, format=fmt, unit=unit, threshold=threshold)
        if result is not None:
            return result, fmt if return_format else result

    return None


@dataclass
@Registry.register
class Timestamp(Converter):

    format: str | None = None
    unit: str = "ms"

    def convert(self, array: Array) -> Conversion | None:

        if not pat.is_string(array.type):
            return None

        result = maybe_parse_timestamps(
            array,
            format=self.format,
            unit=self.unit,
            threshold=self.threshold,
            return_format=True,
        )

        if result is not None:
            converted, format = result
            return Conversion(converted, meta={"format": format})

        return None
