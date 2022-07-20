"""Helpers for pandas compatibility.

Specifically, pyarrow currently doesn't allow to flexibly map types between arrow and pandas
when calling table.to_pandas(). The mapping is per arrow type, and hence one cannot distinguish
between nullable and non-nullable dtypes, and corresponding pandas extension types.

We therefore try to do this mapping via pyarrow's built-in support for pandas metadata.

The below function are analogous to pyarrow functions in its pandas_compat.py module, but instead
of requiring a pandas Series we here only require arrow arrays to create the correct metadata.
"""
from __future__ import annotations

from functools import singledispatch

import pandas as pd
import pyarrow as pa

# from pyarrow import pandas_compat as pdc
from pyarrow import Array, Table
from pyarrow import types as pat

# import json

# from pyarrow.lib import _pandas_api


@singledispatch
def to_pandas(array: Array):
    """Proper conversion allowing pandas extension types."""

    atype = array.type

    if pat.is_string(atype):
        return array.to_pandas().astype("string")

    if pat.is_boolean(atype):
        return array.to_pandas().astype("boolean")

    if pat.is_integer(atype) and array.null_count > 0:
        dtype_name = str(atype).replace("i", "I").replace("u", "U")
        return array.to_pandas(integer_object_nulls=True).astype(dtype=dtype_name)

    return array.to_pandas()


@to_pandas.register
def _(table: Table):
    columns = [to_pandas(array) for array in table]
    df = pd.concat(columns, axis=1)
    df.columns = table.column_names
    return df


def dictionary(array: Array):
    """Return dictionary from a simple or chunked array."""
    if not pat.is_dictionary(array.type):
        raise TypeError(f"Must have (chunked) dictionary array! Got {array.type}.")

    if type(array) == pa.ChunkedArray:
        return array.combine_chunks().dictionary

    return array.dictionary


# def get_extension_dtype_info(array: Array) -> tuple[str, dict]:

#     atype = array.type

#     if pat.is_dictionary(atype):
#         metadata = {
#             "num_categories": len(dictionary(array)),
#             "ordered": atype.ordered,
#         }
#         type_name = str(atype.index_type)
#     elif pat.is_timestamp(atype):
#         metadata = {"timezone": atype.tz}
#         type_name = "datetime64[ns]"
#     else:
#         metadata = None
#         type_name = str(atype)

#     return type_name, metadata


# def get_column_metadata(array: Array) -> dict:
#     """Construct the metadata for a given column"""

#     logical_type = pdc.get_logical_type(array.type)
#     type_name, extra_metadata = get_extension_dtype_info(array)

#     if logical_type == "decimal":
#         extra_metadata = {
#             "precision": array.type.precision,
#             "scale": array.type.scale,
#         }
#         type_name = "object"

#     return {
#         "pandas_type": logical_type,
#         "numpy_type": type_name,
#         "metadata": extra_metadata,
#     }


# def construct_metadata(table: Table) -> dict:
#     """Returns dictionary containing pandas metadata for table columns.

#     The dictionary when used as metadata in a schema can be used to
#     map arrow types to pandas types, e.g. when calling table.to_pandas().
#     """
#     column_metadata = []

#     for name in table.column_names:
#         metadata = get_column_metadata(table.column(name))
#         metadata["name"] = name
#         metadata["field_name"] = name
#         column_metadata.append(metadata)

#     index_column_metadata = []
#     index_descriptors = index_column_metadata = column_indexes = []

#     return {
#         b"pandas": json.dumps(
#             {
#                 "index_columns": index_descriptors,
#                 "column_indexes": column_indexes,
#                 "columns": column_metadata + index_column_metadata,
#                 "creator": {"library": "pyarrow", "version": pa.__version__},
#                 "pandas_version": _pandas_api.version,
#             }
#         ).encode("utf8")
#     }
