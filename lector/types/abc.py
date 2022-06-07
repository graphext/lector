from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import rich.repr
from pyarrow import Array


@dataclass
class Conversion:
    """Return type of a conversion/cast."""

    result: Array
    meta: dict = field(default_factory=dict)


@dataclass
@rich.repr.auto
class Converter(ABC):
    """Simple base class for dependency injection of new custom data types.

    If a proportion of values smaller than `threshold` can be successfully
    converted, the converter should return None.
    """

    threshold: float = 1.0

    @abstractmethod
    def convert(self, arr: Array) -> Conversion | None:
        """To be implemented in subclasses."""


@dataclass
class ConverterRegistry:
    """Registry to manage converters."""

    convs: dict[str, Converter] = field(default_factory=dict)

    def register(self, registered: type) -> type:
        self.convs[registered.__name__.lower()] = registered
        return registered

    def __getitem__(self, item: str) -> Converter:
        return self.convs[item.lower()]


Registry = ConverterRegistry()
"""'Singleton' conversion registry."""
