"""Helpers to pretty print/log objects using Rich."""
from __future__ import annotations

from typing import Iterable, Sequence, TypeVar

import pyarrow.types as pat
from pyarrow import DataType, Schema
from pyarrow import Table as PaTable
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


def schema_comparison(s1: Schema, s2: Schema, title=None, padding=1, left="Before", right="After"):
    meta = {field.name: decode_metadata(field.metadata or {}) for field in s2}
    have_meta = any(meta.values())

    t = Table(title=title, title_justify="left", box=BOX)
    t.add_column("Column", justify="left", style="indian_red1", no_wrap=True)
    t.add_column(left, style="orange1")
    t.add_column(right, style="yellow3")
    if have_meta:
        t.add_column("Meta")

    for field in s2:

        if have_meta:
            field_meta = meta.get(field.name)
            field_meta = Pretty(field_meta) if field_meta else ""

        other = s1.field(field.name)
        if field.type != other.type:
            orig_type = type_view(other.type)
        else:
            orig_type = ""

        t.add_row(field.name, orig_type, type_view(field.type), field_meta)

    return Padding(t, padding)


def schema_diff_view(diff: dict, title=None, padding=1) -> Table:
    """Make a rich view for an arrow schema diff."""

    t = Table(title=title, title_justify="left", box=BOX)
    t.add_column("Column", justify="left", style="indian_red1", no_wrap=True)
    t.add_column("Before", style="orange1")
    t.add_column("After", style="yellow3")

    for col, (before, after) in diff.items():
        t.add_row(col, type_view(before), type_view(after))

    return Padding(t, padding)


def table_view(tbl: PaTable, title=None, max_col_width=20) -> Table:
    """Pyarrow table to rich table."""

    sample = tbl

    n_rows_max = 10
    if sample.num_rows > n_rows_max:
        sample = sample.slice(0, n_rows_max)

    n_cols_max = 5
    if sample.num_columns > n_cols_max:
        sample = sample.select(range(n_cols_max))

    caption = f"{tbl.num_columns} columns, {tbl.num_rows} rows"
    table = Table(
        title=title,
        caption=caption,
        title_justify="left",
        caption_justify="left",
        box=BOX,
    )

    for field in sample.schema:
        name = field.name
        table.add_column(
            name,
            max_width=max_col_width,
            overflow="crop",
            no_wrap=True,
        )

    rows = sample.to_pylist()
    ellipses = len(rows) < tbl.num_rows

    for i, row in enumerate(rows):
        row = [Pretty(x, max_length=max_col_width, max_string=max_col_width) for x in row.values()]
        end_section = False if ellipses else i == len(rows) - 1
        table.add_row(*row, end_section=end_section)

    if ellipses:
        table.add_row(*["..."] * len(rows[0]), end_section=True)

    types = [type_view(field.type) for field in sample.schema]
    nulls = [f"{column.null_count} nulls" for column in sample.columns]
    table.add_row(*types)
    table.add_row(*nulls)

    return table
