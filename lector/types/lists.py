from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.types as pat
from pyarrow import Array, DataType

from ..utils import ensure_type, min_max, proportion_trueish, smallest_int_type
from .abc import Conversion, Converter, Registry

RE_LIST_LIKE: str = r"^[\(\[\|\{<].*[\)\]\|\>}]$"
"""Lists start and end with parenthesis-like characters."""

RE_LIST_CLEAN: str = r"^[\[\{\(\|<]|[\]\}\)\|>]$|['\"\s]"
"""Remove all parenthesis-like characters from start and end. Whitespace and quotes too."""

LIST_TYPES: tuple[str] = (pa.int64(), pa.float64(), pa.timestamp(unit="ms"))


def proportion_listlike(arr: Array) -> float:
    """Calculate proportion of non-null strings that could be lists."""
    valid = arr.drop_null()
    is_list = pac.match_substring_regex(valid, RE_LIST_LIKE)
    return proportion_trueish(is_list)


def maybe_cast_lists(
    arr: Array,
    types: list[DataType | str] = LIST_TYPES,
    downcast: bool = True,
) -> Array | None:
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

    return None


def maybe_parse_lists(
    arr: Array,
    type: str | DataType | None = None,
    threshold: float = 1.0,
) -> Array | None:
    """Parse strings into list, optionally with (inferrable) element type."""

    if proportion_listlike(arr.drop_null()) < threshold:
        return None

    subpat = RE_LIST_CLEAN
    content = pac.replace_substring_regex(arr, pattern=subpat, replacement="")
    result = pac.split_pattern(content, ",")

    if type is not None:
        return result.cast(pa.list_(ensure_type(type)))
    else:
        return maybe_cast_lists(result, types=LIST_TYPES) or result


@dataclass
@Registry.register
class List(Converter):

    type: str | DataType | None = None

    def convert(self, array: Array) -> Conversion | None:

        if not pat.is_string(array.type):
            return None

        result = maybe_parse_lists(array, self.type, self.threshold)
        return Conversion(result) if result else None
