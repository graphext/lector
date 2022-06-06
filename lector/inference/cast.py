"""Helpers to easily cast columns to their most appropriate/efficient type."""
from functools import singledispatch

import pyarrow.types as pat
from pyarrow import Array, DataType, Schema, Table

from ..log import LOG, schema_diff_view, track
from ..utils import proportion_unique
from .dates import maybe_parse_dates
from .lists import maybe_parse_lists, proportion_listlike
from .numbers import autocast_numbers
from .strings import maybe_cast_category, proportion_text, proportion_url


@singledispatch
def autocast(arr: Array) -> Array:
    """Assumes initial arrow inference.

    I.e. numerical columns have already beed inferred correctly.
    """
    if pat.is_floating(arr.type) or pat.is_integer(arr.type):
        return autocast_numbers(arr)

    if pat.is_string(arr.type):

        arr = maybe_parse_dates(arr)
        if pat.is_timestamp(arr.type):
            return arr

        if proportion_listlike(arr) > 0.9:
            arr = maybe_parse_lists(arr, type=None)
            if pat.is_list(arr.type):
                return arr

        if proportion_url(arr) > 0.75:
            # ToDo: Add metadata identfying columnas as URL semantic
            arr = maybe_cast_category(arr, max_cardinality=1.0)
            if pat.is_dictionary(arr.type):
                LOG.print("Encoding URL column as dictionary")
                return arr

        if proportion_text(arr) < 0.9 or proportion_unique(arr) < 0.1:
            arr = maybe_cast_category(arr, max_cardinality=1.0)
            if pat.is_dictionary(arr.type):
                return arr

    return arr


@autocast.register
def _(tbl: Table, log=True) -> Table:

    schema = tbl.schema

    for i, arr in track(enumerate(tbl), total=tbl.num_columns, desc="Autocasting"):

        name = tbl.column_names[i]
        new = autocast(arr)
        if new.type != arr.type:
            tbl = tbl.set_column(i, name, new)

    if log and not schema.equals(tbl.schema):
        diff = schema_diff(schema, tbl.schema)
        LOG.print(schema_diff_view(diff, title="Autocast type changes"))

    return tbl


def schema_diff(s1: Schema, s2: Schema) -> dict[str : tuple[DataType, DataType]]:
    """Check differences in schema's column types."""
    diff = {}

    for field in s1:
        other = s2.field(field.name)
        if field.type != other.type:
            diff[field.name] = (field.type, other.type)

    return diff
