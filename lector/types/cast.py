"""Helpers to easily cast columns to their most appropriate/efficient type."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Iterable, Union

import pyarrow as pa
from pyarrow import Array, Table

from ..log import LOG, schema_diff_view, track
from ..utils import encode_metadata, schema_diff
from .abc import Conversion, Converter, Registry

Config = Dict[str, dict]
"""An (ordered) dict of converter class names and corresponding parameters."""

Converters = Union[Config, Iterable[Converter], None]
"""Accepted argument type where converters are expected."""

DEFAULT_CONVERTERS: Config = {
    "number": {"threshold": 0.95, "allow_unsigned_int": False, "decimal": "."},
    "list": {"threshold": 0.95},
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
    log: bool = True

    def __post_init__(self):
        self.converters = ensure_converters(self.converters)

    # def __init__(self, converters: Converters = None, log: bool = True) -> None:
    #     self.converters = ensure_converters(converters)
    #     self.log = log

    @abstractmethod
    def cast_array(self, array: Array) -> Conversion:
        """Only need to override this."""

    def cast_table(self, table: Table) -> Table:
        """Takes care of updating fields, including metadata etc."""

        schema = table.schema

        for i, array in track(
            enumerate(table),
            total=table.num_columns,
            desc="Autocasting",
            disable=not self.log,
        ):
            name = table.column_names[i]
            # LOG.print(f"Analyzing column '{name}'...")
            conv = self.cast_array(array)

            if conv is not None:
                result = conv.result
                meta = conv.meta or {}
                meta = encode_metadata(meta) if meta else None
                field = pa.field(name, type=result.type, metadata=meta)
                table = table.set_column(i, field, result)

        if self.log:
            diff = schema_diff(schema, table.schema)
            if diff:
                LOG.print(schema_diff_view(diff, title="Changed types"))

        return table

    def cast(self, data: Array | Table) -> Conversion | Table:
        """Shouldn't be necessary, but @singledispatchmethod doesn't work with inheritance."""
        if isinstance(data, Array):
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

    def cast_array(self, array: Array) -> Conversion:

        for converter in self.converters:
            # LOG.print(f"  with converter '{converter}'")
            sample = array.drop_null().slice(length=self.n_samples)
            if len(sample) > 0 and converter.convert(sample):
                if result := converter.convert(array):
                    return result

        LOG.print("  Got no matching converter, will try casting to categorical (dict).")
        try:
            return Conversion(array.dictionary_encode())
        except Exception:
            pass

        return None
