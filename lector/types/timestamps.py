"""Helpers to convert timestamp strings or time-like columns to timestamps.

Arrow seems to be using this parser under the hood:
https://pubs.opengroup.org/onlinepubs/009695399/functions/strptime.html

in its compute.strptime function, which doesn't support timezone offsets via
the %z or %Z directives. Though they do support timezones when importing CSVs
or casting...

TODO:
- Fractional seconds are handled manually, also see
  https://issues.apache.org/jira/browse/ARROW-15883. They are first removed via regex,
  converted to a pyarrow duration type and later added to parsed timestamps.
- Timezones are only supported in format "+0100", but not e.g. "+01:00"

"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array, TimestampScalar, TimestampType

from ..log import LOG
from ..utils import proportion_trueish
from .abc import Conversion, Converter, Registry
from .regex import RE_FRATIONAL_SECONDS

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
    "%a %b %d %H:%M:%S %z %Y",
]

DATE_FORMATS: list[str] = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%a %d %b %Y",
    "%a, %d %b %Y",
]

ISO_FORMAT: str = "ISO8601()"
"""String Arrow recognizes as meaning the ISO format."""

UNIT = "ns"
"""Note that pandas internal unit is fixed to nanoseconds, and with that resolution it can
represent a much smaller period of dates only."""


def timestamp_formats(tz: bool = True) -> list[str]:
    formats = TIMESTAMP_FORMATS
    if tz:
        with_tz = lambda fmt: (fmt, fmt + " %z", fmt + " Z", fmt + " UTC")
        formats = [ext for fmt in formats for ext in with_tz(fmt)]
    formats.extend(DATE_FORMATS)
    return [ISO_FORMAT] + formats


ALL_FORMATS: list[str] = timestamp_formats()
"""All formats tried by default if None is explicitly provided when converting."""


def proportion_fractional_seconds(arr: Array) -> float:
    """Proportion of non-null dates in arr having fractional seconds."""
    valid = arr.drop_null()
    has_frac = pac.match_substring_regex(valid, RE_FRATIONAL_SECONDS)
    return proportion_trueish(has_frac)


def fraction_as_duration(arr: Array) -> Array:
    """Convert an array (of strings) representing fractional seconds to duration type."""

    if pat.is_string(arr.type):
        arr = pac.cast(arr, pa.float64())

    if pat.is_floating(arr.type):
        # Assume values in [0,1]: convert to nanoseconds
        arr = pac.multiply(arr, 1e9)
        arr = pac.trunc(arr)
        arr = pac.cast(arr, pa.int64())

    return pac.cast(arr, pa.duration("ns"))


@lru_cache(maxsize=128, typed=False)
def find_format(ts: TimestampScalar) -> str | None:
    """Try to find the first format that can parse given date."""
    if pac.is_null(ts).as_py():
        return None

    for fmt in ALL_FORMATS:
        try:
            pac.strptime(ts, format=fmt, unit="s")
            return fmt
        except Exception:  # noqa: S112
            continue

    return None


def maybe_parse_known_timestamps(
    arr: Array,
    format: str,
    unit: str = UNIT,
    threshold: float = 1.0,
) -> Array | None:
    """Helper for parsing with known format and no fractional seconds."""

    if threshold == 1.0:  # noqa: PLR2004
        try:
            return pac.strptime(arr, format=format, unit=unit)
        except Exception:
            return None

    valid_before = len(arr) - arr.null_count
    result = pac.strptime(arr, format=format, unit=unit, error_is_null=True)
    valid_after = len(result) - result.null_count

    if (valid_after / valid_before) < threshold:
        return None

    return result


def maybe_parse_timestamps(
    arr: Array,
    format: str | None = None,
    unit: str = UNIT,
    threshold: float = 1.0,
    return_format: bool = False,
) -> Array | None:
    """Parse lists of strings as dates with format inference."""
    min_prop_frac_secs = 0.1

    if proportion_fractional_seconds(arr) > min_prop_frac_secs:
        frac = pac.extract_regex(arr, RE_FRATIONAL_SECONDS)
        frac = pac.struct_field(frac, indices=[0])
        frac = fraction_as_duration(frac)
        arr = pac.replace_substring_regex(arr, RE_FRATIONAL_SECONDS, "")
    else:
        frac = None

    if format is None:
        formats = ALL_FORMATS
        valid = arr.drop_null()

        if len(valid) > 0:
            first_date = valid[0]
            first_format = find_format(first_date)
            if first_format is not None:
                LOG.info(f"Found date format '{first_format}'")
                formats = ALL_FORMATS.copy()
                formats.remove(first_format)
                formats.insert(0, first_format)

    else:
        formats = [format]

    for fmt in formats:
        result = maybe_parse_known_timestamps(arr, format=fmt, unit=unit, threshold=threshold)
        if result is not None:
            if frac is not None:
                result = pac.add(result, frac)
            return (result, fmt) if return_format else result

    return None


@dataclass
@Registry.register
class Timestamp(Converter):
    """Convert string or time-like arrays to timestamp type."""

    format: str | None = None
    """When None, default formats are tried in order."""
    unit: str = UNIT
    """Resolution the timestamps are stored with internally."""
    convert_temporal: bool = True
    """Whether time/date-only arrays should be converted to timestamps."""

    @staticmethod
    def meta(dt: TimestampType) -> dict[str, str]:
        tz = f", {dt.tz}" if dt.tz is not None else ""
        return {"semantic": f"date[{dt.unit}{tz}]"}

    def convert(self, array: Array) -> Conversion | None:
        if (pat.is_time(array.type) or pat.is_date(array.type)) and self.convert_temporal:
            result = array.cast(pa.timestamp(unit=self.unit), safe=False)
            return Conversion(result, self.meta(result.type))

        if pat.is_timestamp(array.type) and array.type.unit != self.unit:
            result = array.cast(pa.timestamp(unit=self.unit), safe=False)
            return Conversion(result, self.meta(result.type))

        if not pat.is_string(array.type):
            return None

        # Pyarrow's strptime behaves different from its internal cast and inference. Only the
        # latter support timezone offset. So try cast first, and then strptime-based conversion.
        try:
            result = pac.cast(array, pa.timestamp(unit=self.unit))
        except pa.ArrowInvalid:
            try:
                result = pac.cast(array, pa.timestamp(unit=self.unit, tz="UTC"))
            except pa.ArrowInvalid:
                result = None

        if result is not None:
            return Conversion(result, self.meta(result.type) | {"format": "arrow"})

        result = maybe_parse_timestamps(
            array,
            format=self.format,
            unit=self.unit,
            threshold=self.threshold,
            return_format=True,
        )

        if result is not None:
            result, format = result
            return Conversion(result, self.meta(result.type) | {"format": format})

        return None
