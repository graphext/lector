"""Helpers to convert timestamp strings or time-like columns to timestamps.

Arrow seems to be using this parser under the hood:
https://pubs.opengroup.org/onlinepubs/009695399/functions/strptime.html

in its compute.strptime function, which doesn't support timezone offsets via
the %z or %Z directives. Though they do support timezones when importing CSVs
or casting...

For arrow internals relating to timestamps also see:

- Timezone internals:
  https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow13TimestampTypeE
- CSV parsing:
  https://arrow.apache.org/docs/cpp/csv.html#timestamp-inference-parsing
- Timestamp umbrella issue:
  https://github.com/apache/arrow/issues/31324

TODO:
- Fractional seconds are handled manually, also see
  https://github.com/apache/arrow/issues/20146. They are first removed via regex,
  converted to a pyarrow duration type and later added to parsed timestamps.
- Timezones are only supported in format "+0100", but not e.g. "+01:00"
- What to do with mixed timezones:
  https://stackoverflow.com/questions/75656639/computing-date-features-using-pyarrow-on-mixed-timezone-data

"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import ClassVar

import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array, TimestampArray, TimestampScalar, TimestampType

from ..log import LOG
from ..utils import proportion_trueish
from .abc import Conversion, Converter, Registry
from .regex import RE_FRATIONAL_SECONDS, RE_TZ_OFFSET

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
    "%d-%m-%y",  # %y first since it will fail with 4 digit years,
    "%d/%m/%y",  # while %Y will not fail on 2 digit years(!)
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


def extract_timezone(timestamps: pa.Array):
    """Extract timezone from a list of string timestamps.

    Currently, the only supported format is +/-HH[:]MM, e.g. +0100.

    Also, returns None if there are multiple different offsets, after
    some basic cleaning. E.g. Z and +0000 are considered the same.
    """
    res = pac.extract_regex(timestamps, RE_TZ_OFFSET)
    res = res.drop_null()

    if not len(res):
        return None

    offsets = pac.struct_field(res, indices=0)
    offsets = pac.replace_substring(offsets, ":", "")
    offsets = pac.replace_substring(offsets, "Z", "+0000")
    offsets = offsets.unique()

    if len(offsets) > 1:
        return None

    offset = offsets[0].as_py()
    return f"{offset[:-2]}:{offset[-2:]}"


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
    """Convert string or time/date-like arrays to timestamp type.

    Note: Arrow will always _parse_ either into UTC or timezone-naive
    timestamps, but never into specific timezones other than UTC
    by default. Also, internally all timestamps are represented as UTC.
    The timezone metadata is then used by other functions to correctly
    extract for example the local day of the week, time etc.

    Non-UTC timestamps can only be created by specifying the TimestampType
    explicitly, or using the assume_timezone function.

    When converting to pandas, the timezone is handled correctly.

    When input strings have no explicit timezone information, uses `tz`
    parameter to interpret them as local to that tz. If tz=None, keeps
    them as timezone-naive timestamps. If input strings do have explicit
    timezone information, will be represented internally as UTC (as always),
    and simply set the tz metadata so that component extraction etc. will
    use correctly localized moments in time.

    TZ-naive timestamps ["2013-07-17 05:00", "2013-07-17 02:00"]:

        - assume_timezone(NY): interprets input timestamps as local to tz,
            converts and stores them as UTC, and keeps tz metadata for
            correct localization when printing/extracting components. I.e.,
            will convert to [2013-07-17 09:00:00, 2013-07-17 06:00:00] UTC,
            but when needed, will localize on demand to
            [2013-07-17 05:00:00-04:00 2013-07-17 02:00:00-04:00].

        - cast with timezone(NY): interprets input timestamps as local to UTC,
            and stores the tz as metadata for on-demand localization. I.e.,
            timestamps will be [2013-07-17 05:00:00, 2013-07-17 02:00:00] UTC,
            and when needed will localize on demand to
            [2013-07-17 01:00:00-04:00 2013-07-16 22:00:00-04:00].

    TZ-aware timestamps ["2013-07-17 05:00", "2013-07-17 02:00"] UTC:

        - cast with timezone(NY): since input timestamps internally are already
            always in UTC, keeps them as UTC ["2013-07-17 05:00", "2013-07-17 02:00"],
            but localizes to cast tz on demand, i.e. to
            [2013-07-17 01:00:00-04:00 2013-07-16 22:00:00-04:00].
    """

    format: str | None = None
    """When None, default formats are tried in order."""
    unit: str = UNIT
    """Resolution the timestamps are stored with internally."""
    tz: str | None = None
    """The desired timezone of the timestamps."""
    convert_temporal: bool = True
    """Whether time/date-only arrays should be converted to timestamps."""

    DEFAULT_TZ: ClassVar[str] = "UTC"

    @staticmethod
    def meta(dt: TimestampType) -> dict[str, str]:
        tz = f", {dt.tz}" if dt.tz is not None else ""
        return {"semantic": f"date[{dt.unit}{tz}]"}

    @staticmethod
    def to_timezone(array: TimestampArray, tz: str | None) -> TimestampArray:
        if tz is not None:
            if array.type.tz is None:
                # Interpret as local moments in given timezone to convert to UTC equivalent
                return pac.assume_timezone(
                    array, timezone=tz, ambiguous="earliest", nonexistent="earliest"
                )

            # Keep UTC internally, simply change what local time is assumed in temporal functions
            return array.cast(pa.timestamp(unit=array.type.unit, tz=tz))

        if array.type.tz is not None:
            # Make local timezone-naive. Careful: the following will make the timestamps
            # naive, but with local time in UTC, not using the existing timezone metadata!
            # return array.cast(pa.timestamp(unit=array.type.unit, tz=None))  # noqa: ERA001
            raise NotImplementedError("Pyarrow's to_local() will not be implemented until v12.0!")

        # Keep as timezone-naive timestamps
        return array

    def convert_date_time(self, array: Array) -> Conversion | None:
        try:
            result = array.cast(pa.timestamp(unit=self.unit), safe=False)
            result = self.to_timezone(result, self.tz or self.DEFAULT_TZ)
            return Conversion(result, self.meta(result.type))
        except pa.ArrowNotImplementedError:
            LOG.error(f"Pyarrow cannot convert {array.type} to timestamp!")
            return None

    def convert_timestamp(self, array: Array) -> Conversion | None:
        result = array
        if array.type.unit != self.unit:
            result = array.cast(pa.timestamp(unit=self.unit), safe=False)

        result = self.to_timezone(result, self.tz or self.DEFAULT_TZ)
        return Conversion(result, self.meta(result.type))

    def convert_strings(self, array: Array) -> Conversion | None:
        try:
            # Pyarrow's strptime behaves different from its internal cast and inference. Only the
            # latter support timezone offset. So try cast first, and then strptime-based conversion.
            result = array.cast(pa.timestamp(unit=self.unit))
        except pa.ArrowInvalid:
            try:
                result = array.cast(pa.timestamp(unit=self.unit, tz="UTC"))
            except pa.ArrowInvalid:
                result = None

        if result is not None:
            tz = self.tz or extract_timezone(array)
            result = self.to_timezone(result, tz or self.DEFAULT_TZ)
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
            result = self.to_timezone(result, self.tz)
            return Conversion(result, self.meta(result.type) | {"format": format})

        return None

    def convert(self, array: Array) -> Conversion | None:
        if (pat.is_time(array.type) or pat.is_date(array.type)) and self.convert_temporal:
            return self.convert_date_time(array)

        if pat.is_timestamp(array.type):
            return self.convert_timestamp(array)

        return self.convert_strings(array) if pat.is_string(array.type) else None
