"""Helpers to pretty print/log objects using Rich."""
from __future__ import annotations

from typing import Iterable, Sequence, TypeVar

import pyarrow.types as pat
from pyarrow import DataType, Schema
from rich import box, get_console
from rich.padding import Padding
from rich.panel import Panel
from rich.pretty import Pretty
from rich.progress import Progress, TimeElapsedColumn
from rich.table import Column, Table

from .utils import decode_metadata

LOG = get_console()

BOX = box.HEAVY_HEAD

Item = TypeVar("Item")


def track(
    items: Iterable[Item] | Sequence[Item],
    columns: Iterable[Column] | None = None,
    total: float | None = None,
    desc: str = "Processing",
    update_period: float = 0.1,
    **kwds,
) -> Iterable[Item]:
    """Rich track with elapsed time by default."""
    if columns is None:
        columns = (*Progress.get_default_columns(), TimeElapsedColumn())

    with Progress(*columns, **kwds) as progress:
        yield from progress.track(
            items,
            total=total,
            description=desc,
            update_period=update_period,
        )


def type_view(type: DataType) -> str:
    """More compact strinf represenation of arrow data types."""
    if pat.is_list(type):
        return f"list<{type.value_type}>"
    if pat.is_dictionary(type):
        return f"dict<{type.value_type}, {type.ordered}>"
    return str(type)


def dict_view(d: dict, title="", expand=False, **kwds) -> Panel:
    dv = Pretty(d, **kwds)
    return Panel(dv, expand=expand, title=title, box=box.HEAVY_HEAD)


def schema_view(schema: Schema, title=None, padding=1) -> Table:
    """Make a rich view for arrow schema."""

    meta = {field.name: decode_metadata(field.metadata or {}) for field in schema}
    have_meta = any(meta.values())

    rt = Table(title=title, title_justify="left", box=BOX)
    rt.add_column("Column", justify="left", style="indian_red1", no_wrap=True)
    rt.add_column("Type", style="yellow3")
    if have_meta:
        rt.add_column("Meta")

    for field in schema:
        if have_meta:
            field_meta = meta.get(field.name)
            field_meta = Pretty(field_meta) if field_meta else None
            rt.add_row(field.name, type_view(field.type), field_meta)
        else:
            rt.add_row(field.name, type_view(field.type))

    return Padding(rt, padding)


def schema_diff_view(diff: dict, title=None, padding=1) -> Table:
    """Make a rich view for an arrow schema diff."""

    t = Table(title=title, title_justify="left", box=BOX)
    t.add_column("Column", justify="left", style="indian_red1", no_wrap=True)
    t.add_column("Before", style="orange1")
    t.add_column("After", style="yellow3")

    for col, (before, after) in diff.items():
        t.add_row(col, type_view(before), type_view(after))

    return Padding(t, padding)
