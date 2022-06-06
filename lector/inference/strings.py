from __future__ import annotations

from math import inf as INF

import pyarrow.compute as pac
from pyarrow import Array

from ..utils import Number, proportion_trueish
from .lists import RE_LIST_LIKE

MAX_CARDINALITY: Number = 0.1
"""Maximum cardinalty for categoricals (arrow's default is 50 in ConvertOptions)."""

TEXT_MIN_SPACES: Number = 2
"""Strings need to have this many spaces to be considered text."""

TEXT_MIN_LENGTH: Number = 15
"""Strings need to be this long to be considered text."""

TEXT_IGNORE_LISTS: bool = True
"""Whether to count list-like strings as texts."""

TEXT_PROPORTION_THRESHOLD: float = 0.8
"""Infer text type if a proportion or values greater than this is text-like. """

RE_URL = (
    r"^(http://www\.|https://www\.|http://|https://)?"  # http:// or https://
    # r'^(https?://(www\.)?)?'  # http:// or https://
    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"  # domain...
    r"localhost|"  # localhost...
    r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # ...or ip
    r"(?::\d+)?"  # optional port
    r"(?:/?|[/?]\S+)$"
)


def is_text(
    arr: Array,
    min_spaces: int = TEXT_MIN_SPACES,
    min_length: int = TEXT_MIN_LENGTH,
    ignore_lists: bool = TEXT_IGNORE_LISTS,
) -> bool:
    """Check for natural language-like texts using criteria like lengths, number of spaces."""
    is_long = pac.greater_equal(pac.utf8_length(arr), min_length)
    has_spaces = pac.greater_equal(pac.count_substring_regex(arr, pattern=r"\s"), min_spaces)
    textlike = pac.and_(is_long, has_spaces)

    if ignore_lists:
        listlike = pac.match_substring_regex(arr, RE_LIST_LIKE)
        return pac.and_not(textlike, listlike)

    return textlike


def proportion_text(
    arr: Array,
    min_spaces: int = TEXT_MIN_SPACES,
    min_length: int = TEXT_MIN_LENGTH,
    ignore_lists: bool = TEXT_IGNORE_LISTS,
) -> float:
    """Calculate proportion of natural language-like texts given criteria."""
    is_txt = is_text(arr.drop_null(), min_spaces, min_length, ignore_lists)
    return proportion_trueish(is_txt)


def proportion_url(arr: Array) -> float:
    """Use regex to find proportion of strings that are (web) URL-like."""
    is_url = pac.match_substring_regex(arr.drop_null(), RE_URL, ignore_case=True)
    return proportion_trueish(is_url)


def maybe_cast_category(arr: Array, max_cardinality: Number = MAX_CARDINALITY) -> Array:
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

    return arr
