from __future__ import annotations

import pyarrow.compute as pac
from pyarrow import Array, TimestampScalar

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
    n_fracs = pac.sum(has_frac).as_py()
    return n_fracs / len(valid)


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


def maybe_parse_dates(arr: Array, format: str | None = None, unit: str = "ms") -> Array:
    """Parse lists of strings as dates with format inference."""

    if proportion_trailing_decimals(arr) > 0.1:
        split = pac.split_pattern(arr, ".", max_splits=1, reverse=True)
        arr = pac.list_element(split, 0)

    if format is None:

        formats = ALL_FORMATS

        # Try to move first matching format to front
        non_null = arr.drop_null()
        if len(non_null) > 0:
            first_date = non_null[0]
            first_format = find_format(first_date)
            if first_format is not None:
                print(f"First date '{first_date}' matches {first_format}")
                formats = ALL_FORMATS.copy()
                formats.remove(first_format)
                formats.insert(0, first_format)

        for i, fmt in enumerate(formats):
            try:
                return pac.strptime(arr, format=fmt, unit=unit)
            except Exception:
                pass

        return arr

    try:
        return pac.strptime(arr, format=format, unit=unit)
    except Exception:
        return arr
