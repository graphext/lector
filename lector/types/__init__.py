"""Subpackage for inferring column types in CSV files.

This is instead or on top of Arrow's built-in inference, which currently doesn't detect
list columns, timestamps in non-ISO formats, or semantic types such as URLs, natural language
text etc.
"""
from .abc import Converter, Registry
from .cast import Autocast, Cast
from .lists import List
from .numbers import Number
from .strings import Category, Text, Url
from .timestamps import Timestamp

"""Note, we need to import the types here, otherwise they won't be registered."""

__all__ = [
    "Autocast",
    "Cast",
    "Converter",
    "Registry",
    "Category",
    "List",
    "Number",
    "Text",
    "Timestamp",
    "Url",
]
