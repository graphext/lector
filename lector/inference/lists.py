from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pac
from pyarrow import Array, DataType

from ..utils import ensure_type, min_max, smallest_int_type

RE_LIST_LIKE: str = r"^[\(\[\|\{<].*[\)\]\|\>}]$"
"""Lists start and end with parenthesis-like characters."""

RE_LIST_CLEAN: str = r"^[\[\{\(\|<]|[\]\}\)\|>]$|['\"\s]"
"""Remove all parenthesis-like characters from start and end. Whitespace and quotes too."""

LIST_TYPES: tuple[str] = (pa.int64(), pa.float64(), pa.timestamp(unit="ms"))


def proportion_listlike(arr: Array) -> float:
    """Calculate proportion of non-null strings that could be lists."""
    valid = arr.drop_null()
    pattern = RE_LIST_LIKE
    is_list = pac.match_substring_regex(valid, pattern)
    n_lists = is_list.combine_chunks().sum().as_py()
    return n_lists / len(valid)


def maybe_cast_lists(
    arr: Array,
    types: list[DataType | str] = LIST_TYPES,
    downcast: bool = True,
) -> Array:
    """Cast lists (of strings) to first valid type, if any."""
    for type in types:
        type = ensure_type(type)
        try:
            result = pac.cast(arr, pa.list_(type))

            if type == "int64" and downcast:
                vmin, vmax = min_max(pac.list_flatten(result))
                itype = smallest_int_type(vmin, vmax)

                if itype is not None:
                    try:
                        itype = ensure_type(itype)
                        result = pac.cast(result, pa.list_(itype))
                    except Exception as exc:
                        print(exc)
                        print("Will not downcast lists of int64.")

            return result
        except Exception:
            pass

    return arr


def maybe_parse_lists(arr: Array, type: str | DataType | None = None) -> Array:
    """Parse strings into list, optionally with (inferrable) element type."""

    if proportion_listlike(arr) > 0.9:

        subpat = RE_LIST_CLEAN
        content = pac.replace_substring_regex(arr, pattern=subpat, replacement="")
        result = pac.split_pattern(content, ",")

        if type is not None:
            type = ensure_type(type)
            result = result.cast(pa.list_(type))
        else:
            result = maybe_cast_lists(result, types=LIST_TYPES)

        return result

    return arr
