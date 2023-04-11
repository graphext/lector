import io
from collections import namedtuple

import pyarrow as pa
import pyarrow.types as pat

import lector
from lector import ArrowReader, Autocast

from .utils import equal

TC = namedtuple("TC", "min,max,extra,null")
"""Test Case definition."""

TYPE_COLUMNS = {
    "num_int8": TC("-128", "127", "0", "NA"),
    "num_int16": TC("-32768", "32767", "0", "NA"),
    "num_int32": TC("-2147483648", "2147483647", "0", "NA"),
    "num_int64": TC("-9223372036854775808", "9223372036854775807", "0", "NA"),
    "num_uint8": TC("0", "255", "1", "NA"),
    "num_uint16": TC("0", "65535", "1", "NA"),
    "num_uint32": TC("0", "4294967295", "1", "NA"),
    # "num_uint64": TS("0", "18446744073709551615", "", "NA"),  # noqa
    "list_uint8": TC('"[0,1,2]"', '"[7,8,9]"', '"[]"', "NA"),
    "list_uint8_2": TC('"[0,1,2]"', '"[7,8,9]"', '"[4]"', "NA"),
    "list_str": TC('"[a,b,c]"', '"[x,y,z]"', '"[]"', "NA"),
    "list_str_2": TC('"[a,b,c]"', '"[x,y,z]"', '"[test]"', "NA"),
    "date_iso": TC(
        "2022-06-17T10:31:40.000746",
        "2022-06-17T10:31:40.000746",
        "2022-06-17T10:31:40.000000",
        "NA",
    ),
    "date_custom": TC(
        "2022-06-17",
        "2022-06-17",
        "2022-05-03",
        "NA",
    ),
    "text": TC(
        '"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor."',
        '"No one rejects, dislikes, or avoids pleasure itself, because it is pleasure."',
        '"Nor again is there anyone who loves or pursues or desires to obtain pain of itself."',
        "NA",
    ),
    "cat": TC("cat1", "cat2", "", "NA"),
}

TYPE_CSV = ",".join(TYPE_COLUMNS)
for row in map(list, zip(*TYPE_COLUMNS.values())):
    TYPE_CSV += "\n" + ",".join(row)

ARROW_TYPES = {
    "num_int8": pa.int64(),
    "num_int16": pa.int64(),
    "num_int32": pa.int64(),
    "num_int64": pa.int64(),
    "num_uint8": pa.int64(),
    "num_uint16": pa.int64(),
    "num_uint32": pa.int64(),
    "list_uint8": pa.string(),
    "list_uint8_2": pa.string(),
    "list_str": pa.string(),
    "list_str_2": pa.string(),
    "date_iso": pa.timestamp(unit="ns"),
    "date_custom": pa.date32(),
    "text": pa.string(),
    "cat": pa.string(),
}

LECTOR_TYPES = {
    "num_int8": pa.int8(),
    "num_int16": pa.int16(),
    "num_int32": pa.int32(),
    "num_int64": pa.int64(),
    "num_uint8": pa.uint8(),
    "num_uint16": pa.uint16(),
    "num_uint32": pa.uint32(),
    "list_uint8": pa.list_(pa.uint8()),
    "list_uint8_2": pa.list_(pa.uint8()),
    "list_str": pa.list_(pa.string()),
    "list_str_2": pa.list_(pa.string()),
    "date_iso": pa.timestamp(unit="ns", tz="UTC"),
    "date_custom": pa.timestamp(unit="ns", tz="UTC"),
    "text": pa.string(),
    "cat": pa.dictionary(index_type=pa.int32(), value_type=pa.string()),
}

DELIM_CSV = b"""
dot_delim, comma_delim, mixed_delim_dot, mixed_delim_comma, mixed_delim
"1,234.0","1.234,0","1,234.0","1.234,0","1.234,0"
"1,234,456",1.234.456,"1,234,456",1.234.456,1.234.456
NA,NA,NA,NA,NA
"1,234,456.987","1.234.456,987","1,234,456.987","1.234.456,987","1.234.456,987"
0.1,"0,1","0.1","0,1",0.1
.1,",1",",1",.1,.1
98765.123,"98765,123","98765,123",98765.123,98765.123
"""

# ruff: noqa: E501
LIST_CSV = """
lnum1,lnum2,lnum_NA_3,lnum4,lcat5,lfloat6,lfloat7,lfloat8,lfloat_DEL_9
"[0,1,2]","[0,1,2]","['123', '456', NA, '789']","[123, 456, 789]","[a,b,c]","[123.45, 678.90]","[""123.45"", ""678.90""]","['123.45', '678.90']","['123,45', '678,90']"
"[7,8,9]","[7,8,9]","['123', '456', NA, '789']","[123, 456, 789]","(d,e)","[123, 678]","[""123"", ""678""]","['123', '678']","['123', '678']"
"[]","[4]","[123, 456, NA, 789]","[123, 456, 789]","<f>","[123.45, 678.90]","[""123.45"", ""678.90""]","['123.45', '678.90', '0.0']","['123,45', '678,90', '0,0']"
"NA","NA",NA,NA,NA,NA,NA,NA,NA
"""


def test_decimals():
    """Based on inferred decimal delimiter, thousands delimiter gets removed.

    If delimiter is ambiguous, result will be dict.
    """
    tbl = lector.read_csv(io.BytesIO(DELIM_CSV))

    for i in range(4):
        assert pat.is_floating(tbl.column(i).type)

    assert pat.is_dictionary(tbl.column(4).type)


def test_list():
    """List parsing. NAs are not allowed in float lists. Also, decimal delimiter must be the period character!"""
    fp = io.BytesIO(LIST_CSV.encode("utf-8"))
    tbl = lector.read_csv(fp)

    exp_types = {
        "lnum1": pa.list_(pa.uint8()),
        "lnum2": pa.list_(pa.uint8()),
        "lnum_NA_3": pa.list_(pa.string()),  # NA not supported in numeric lists
        "lnum4": pa.list_(pa.uint16()),
        "lcat5": pa.list_(pa.string()),
        "lfloat6": pa.list_(pa.float64()),
        "lfloat7": pa.list_(pa.float64()),
        "lfloat8": pa.list_(pa.float64()),
        "lfloat_DEL_9": pa.list_(
            pa.uint16()
        ),  # comma as decimal delimiter not supported (interpreted as csv delimter)
    }

    for col in tbl.column_names:
        assert tbl.column(col).type == exp_types[col]


def test_inference():
    """Compare Arrow and Lector inferred types against reference.

    TODO:
    - Deal correctly with ns/ms in timestamps.
    - Default dictionary encoding uses int32 always. Downcast?
    """
    file = io.BytesIO(TYPE_CSV.encode("utf-8"))
    tbl = ArrowReader(file, log=False).read()
    schema = tbl.schema

    for name, type in ARROW_TYPES.items():
        assert equal(type, schema.field(name).type, extra=name)

    tbl = Autocast(log=False).cast(tbl)
    schema = tbl.schema

    for name, type in LECTOR_TYPES.items():
        assert equal(type, schema.field(name).type, extra=name)
