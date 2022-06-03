"""Helpers to easily cast columns to their most appropriate/efficient type."""
from functools import singledispatch

import pyarrow.types as pat
from pyarrow import Array, Table

from ..utils import proportion_unique
from .dates import maybe_parse_dates
from .lists import maybe_parse_lists
from .numbers import autocast_numbers
from .strings import maybe_cast_category, proportion_text


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

        arr = maybe_parse_lists(arr, type=None)
        if pat.is_list(arr.type):
            return arr

        if proportion_text(arr) < 0.9 or proportion_unique(arr) < 0.1:
            arr = maybe_cast_category(arr, max_cardinality=1.0)
            if pat.is_dictionary(arr.type):
                return arr

    return arr


@autocast.register
def _(tbl: Table) -> Table:

    for i, arr in enumerate(tbl):

        name = tbl.column_names[i]
        new = autocast(arr)

        if new.type != arr.type:
            tbl = tbl.set_column(i, name, new)

    return tbl
