"""Helpers to easily cast columns to their most appropriate/efficient type."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Iterable, Union

import pyarrow as pa
from pyarrow import Array, ChunkedArray, Table

from ..log import LOG, schema_diff_view, track
from ..utils import encode_metadata, schema_diff
from .abc import Conversion, Converter, Registry
from .strings import Category

Config = Dict[str, dict]
"""An (ordered) dict of converter class names and corresponding parameters."""

Converters = Union[Config, Iterable[Converter], None]
"""Accepted argument type where converters are expected."""

DEFAULT_CONVERTERS: Config = {
    "number": {"threshold": 0.95, "allow_unsigned_int": True, "decimal": "."},
    "list": {"threshold": 0.95, "threshold_urls": 0.8},
    "timestamp": {"threshold": 0.95},
    "text": {"threshold": 0.8, "min_unique": 0.1},
    "url": {"threshold": 0.8},
    "category": {"threshold": 0.0, "max_cardinality": None},
}


def ensure_converters(converters: Converters = None) -> list[Converter]:
    """Turn a type conversion config into a list of converter instances."""
    if converters is None:
        converters = DEFAULT_CONVERTERS.copy()

    if isinstance(converters, dict):
        return [Registry[name](**params) for name, params in converters.items()]

    if isinstance(converters, list) and converters and isinstance(converters[0], Converter):
        return converters

    raise ValueError(f"Object cannot be made into type converters: {converters}")


@dataclass
class CastStrategy(ABC):
    """Base class for autocasting implementations."""

    converters: Converters | None = None
    columns: list[str] | None = None
    log: bool = False

    def __post_init__(self):
        self.converters = ensure_converters(self.converters)

    @abstractmethod
    def cast_array(self, array: Array, name: str | None = None) -> Conversion:
        """Only need to override this."""

    def cast_table(self, table: Table) -> Table:
        """Takes care of updating fields, including metadata etc."""

        schema = table.schema
        columns = self.columns or table.column_names

        for name in track(columns, desc="Autocasting", disable=not self.log):

            array = table.column(name)
            conv = self.cast_array(array, name=name)

            if conv is not None:
                result = conv.result
                meta = conv.meta or {}
                meta = encode_metadata(meta) if meta else None
                field = pa.field(name, type=result.type, metadata=meta)
                table = table.set_column(table.column_names.index(name), field, result)

        if self.log:
            diff = schema_diff(schema, table.schema)
            if diff:
                LOG.print(schema_diff_view(diff, title="Changed types"))

        return table

    def cast(self, data: Array | ChunkedArray | Table) -> Conversion | Table:
        """Shouldn't be necessary, but @singledispatchmethod doesn't work with inheritance."""
        if isinstance(data, (Array, ChunkedArray)):
            return self.cast_array(data)
        elif isinstance(data, Table):
            return self.cast_table(data)

        raise ValueError(f"Can only cast arrays or tables, got {type(data)}!")


@dataclass
class Autocast(CastStrategy):
    """Simple cast trying each registered type in order.

    As a little performance optimization (having a huge effect on execution time),
    types are first tested on a sample for fast rejection of non-matching types.
    """

    n_samples: int = 100

    def cast_array(self, array: Array | ChunkedArray, name: str | None = None) -> Conversion:

        # if self.log:
        #     LOG.print(f"Converting column {name or ''}")

        for converter in self.converters:

            # if self.log:
            #     LOG.print(f"  with converter '{converter}'")

            sample = array.drop_null().slice(length=self.n_samples)
            if len(sample) > 0 and converter.convert(sample):
                if result := converter.convert(array):
                    if self.log:
                        LOG.print(f"Converted column {name or ''} with {converter}")
                    return result

        if pa.types.is_string(array.type):
            LOG.print(
                f"Got no matching converter for string column '{name or ''}'. "
                "Will try casting to categorical (dict)."
            )
            return Category(threshold=0.0, max_cardinality=None).convert(array)

        return None


@dataclass
class Cast:
    """Tried a specific cast for each columns."""

    converters: dict[str, Converter]
    log: bool = False

    def cast(self, table: Table) -> Table:

        schema = table.schema

        for i, (name, converter) in track(
            enumerate(self.converters.items()),
            total=len(self.converters),
            desc="Explicit casting",
            disable=not self.log,
        ):
            array = table.column(name)
            conv = converter.convert(array)

            if conv is not None:
                result = conv.result
                meta = conv.meta or {}
                meta = encode_metadata(meta) if meta else None
                field = pa.field(name, type=result.type, metadata=meta)
                idx = table.schema.get_field_index(name)
                table = table.set_column(idx, field, result)
            else:
                LOG.print(f"Conversion of columns '{name}' with converter '{converter}' failed!")
                LOG.print(f"Original column type: {array.type}")
                LOG.print(f"Original column: {array}")

        if self.log:
            diff = schema_diff(schema, table.schema)
            if diff:
                LOG.print(schema_diff_view(diff, title="Changed types"))

        return table
