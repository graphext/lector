"""Helpers to convert to types that logically remain strings (e.g. categoricals).

TODO:

 - Find a fast way to recognize whitespaces with regex (see is_text)
 - Try faster early out for text recognition using sufficient_texts()

"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from math import inf as INF

import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array

from ..log import LOG
from ..utils import Number, map_values, proportion_trueish, proportion_unique, sorted_value_counts
from .abc import Conversion, Converter, Registry
from .regex import RE_LIST_LIKE, RE_URL

MAX_CARDINALITY: Number = 0.1
"""Maximum cardinalty for categoricals (arrow's default is 50 in ConvertOptions)."""

TEXT_MIN_SPACES: Number = 2
"""Strings need to have this many spaces to be considered text."""

TEXT_MIN_LENGTH: Number = 15
"""Strings need to be this long to be considered text."""

TEXT_REJECT_LISTS: bool = True
"""Whether to count list-like strings as texts."""

TEXT_PROPORTION_THRESHOLD: float = 0.8
"""Infer text type if a proportion or values greater than this is text-like. """


def is_text(
    arr: Array,
    min_spaces: int = TEXT_MIN_SPACES,
    min_length: int = TEXT_MIN_LENGTH,
    reject_lists: bool = TEXT_REJECT_LISTS,
) -> bool:
    """Check for natural language-like texts using criteria like lengths, number of spaces."""
    is_long = pac.greater_equal(pac.utf8_length(arr), min_length)
    # This regex seems to be very slow
    # has_spaces = pac.greater_equal(pac.count_substring_regex(arr, pattern=r"\s"), min_spaces)
    has_spaces = pac.greater_equal(pac.count_substring(arr, pattern=" "), min_spaces)
    textlike = pac.and_(is_long, has_spaces)

    if reject_lists:
        listlike = pac.match_substring_regex(arr, RE_LIST_LIKE)
        return pac.and_not(textlike, listlike)

    return textlike


def proportion_text(
    arr: Array,
    min_spaces: int = TEXT_MIN_SPACES,
    min_length: int = TEXT_MIN_LENGTH,
    reject_lists: bool = TEXT_REJECT_LISTS,
) -> float:
    """Calculate proportion of natural language-like texts given criteria."""
    is_txt = is_text(arr.drop_null(), min_spaces, min_length, reject_lists)
    return proportion_trueish(is_txt)


def sufficient_texts(
    arr: Array,
    min_spaces: int = TEXT_MIN_SPACES,
    min_length: int = TEXT_MIN_LENGTH,
    reject_lists: bool = TEXT_REJECT_LISTS,
    threshold: float = 1.0,
) -> bool:
    """Check for natural language-like texts using criteria like lengths, number of spaces.

    This is different from above in that for each text condition, we can early out if the
    condition is not met, without evaluating the remaining conditions. I.e., should be faster.
    """
    is_long = pac.greater_equal(pac.utf8_length(arr), min_length)
    if proportion_trueish(is_long) < threshold:
        return False

    # This regex seems to be very slow
    # has_spaces = pac.greater_equal(pac.count_substring_regex(arr, pattern=r"\s"), min_spaces)
    has_spaces = pac.greater_equal(pac.count_substring(arr, pattern=" "), min_spaces)
    if proportion_trueish(has_spaces) < threshold:
        return False

    if reject_lists:
        is_listlike = pac.match_substring_regex(arr, RE_LIST_LIKE)
        if proportion_trueish(is_listlike) > (1.0 - threshold):
            return False

    return True


def proportion_url(arr: Array) -> float:
    """Use regex to find proportion of strings that are (web) URL-like."""
    is_url = pac.match_substring_regex(arr.drop_null(), RE_URL, ignore_case=True)
    return proportion_trueish(is_url)


def maybe_cast_category(
    arr: Array,
    max_cardinality: Number | None = MAX_CARDINALITY,
) -> Array | None:
    """Cast to categorical depending on cardinality and whether strings are text-like."""

    if max_cardinality is None or max_cardinality == INF:
        return pac.dictionary_encode(arr)

    n_unique = pac.count_distinct(arr, mode="only_valid").as_py()

    if max_cardinality > 1:
        do_cast = n_unique <= max_cardinality
    elif max_cardinality > 0:
        n_valid = len(arr) - arr.null_count
        do_cast = (n_unique / n_valid) <= max_cardinality
    else:
        do_cast = False

    if max_cardinality is None or do_cast:
        return pac.dictionary_encode(arr)

    return None


@dataclass
@Registry.register
class Text(Converter):
    """Anything could be text, but we can enforce text-likeness and uniqueness."""

    min_unique: float = 0.1

    def convert(self, array: Array) -> Conversion | None:
        if not pat.is_string(array.type):
            return None

        if proportion_unique(array) >= self.min_unique:
            if proportion_text(array) >= self.threshold:
                # if sufficient_texts(array, self.threshold):
                return Conversion(array, meta={"semantic": "text"})

        return None


@dataclass
@Registry.register
class Url(Converter):
    """Anything could be text, but we can enforce text-likeness and uniqueness."""

    def convert(self, array: Array) -> Conversion | None:
        if not pat.is_string(array.type):
            return None

        if proportion_url(array) >= self.threshold:
            result = pac.dictionary_encode(array)
            return Conversion(result, meta={"semantic": "url"})

        return None


@dataclass
@Registry.register
class Category(Converter):
    """Anything could be text, but we can enforce text-likeness and uniqueness."""

    max_cardinality: Number | None = MAX_CARDINALITY

    def convert(self, array: Array) -> Conversion | None:
        if not pat.is_string(array.type):
            return None

        result = maybe_cast_category(array, self.max_cardinality)
        return Conversion(result, meta={"semantic": "category"}) if result else None


class Sex(Enum):
    Female = 0
    Male = 1


class SexMapper:
    """Infer values encoding a person's sex in a column and map to configurable labels."""

    DEFAULT_VALUES = {
        Sex.Female: ["female", "f", "femenino", "mujer", "m"],
        Sex.Male: ["male", "m", "masculino", "hombre", "varón", "varon", "h", "v"],
    }

    def __init__(self, values: tuple[str, str], labels: dict[Sex, str] | None = None):
        self.labels = labels or {Sex.Female: "Female", Sex.Male: "Male"}
        self.infer_values(values)
        self.make_mapping()

    def infer_values(self, values: tuple[str, str]) -> dict:
        """Infer which values encode female/male categories."""
        if len(values[0]) == 1 and len(values[1]) == 1 and "m" in values:

            f_label, m_label = self.labels[Sex.Female], self.labels[Sex.Male]

            if "f" in values:
                # male/female or masculino/femenino
                self.values = {Sex.Female: ["f", f_label], Sex.Male: ["m", m_label]}
            elif "v" in values:
                # mujer/varon
                self.values = {Sex.Female: ["m", f_label], Sex.Male: ["v", m_label]}
            elif "h" in values:
                # mujer/hombre
                self.values = {Sex.Female: ["m", f_label], Sex.Male: ["h", m_label]}

        self.values = self.DEFAULT_VALUES

    def make_mapping(self) -> dict[str, str]:
        """Create a mapping from inferred values to desired labels."""
        ensure_list = lambda x: x if isinstance(x, list) else [x]
        self.map = {val: self.labels[sex] for sex in Sex for val in ensure_list(self.values[sex])}


def maybe_sex(arr: Array) -> tuple[str, str] | None:
    """Check if the two most common values are sex-like and return them."""
    lower = pac.utf8_lower(arr)
    top2 = sorted_value_counts(lower, top_n=2)
    values = top2.field("values").to_pylist()

    if len(values) == 2:
        mapper = SexMapper(values)
        LOG.print(f"Sex mapping: {mapper.map}")
        mapped = map_values(lower, mapper.map)
        return mapped.dictionary_encode()

    return arr
