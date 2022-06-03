import pyarrow.types as pat
from pyarrow import DataType, Schema
from rich import get_console
from rich.table import Table

LOG = get_console()


def type_view(type: DataType) -> str:
    """More compact strinf represenation of arrow data types."""
    if pat.is_list(type):
        return f"list<{type.value_type}>"
    if pat.is_dictionary(type):
        return f"dict<{type.value_type}, {type.ordered}>"
    return str(type)


def schema_view(schema: Schema, title=None) -> Table:
    """Make a rich view for arrow schema."""

    t = Table(title=title, title_justify="left")
    t.add_column("Column", justify="left", style="cyan", no_wrap=True)
    t.add_column("Type", style="magenta")

    for field in schema:
        t.add_row(field.name, type_view(field.type))

    return t


def schema_diff_view(diff: dict, title=None) -> Table:
    """Make a rich view for an arrow schema diff."""

    t = Table(title=title, title_justify="left")
    t.add_column("Column", justify="left", style="cyan", no_wrap=True)
    t.add_column("Before", style="magenta")
    t.add_column("After", style="magenta")

    for col, (before, after) in diff.items():
        t.add_row(col, type_view(before), type_view(after))

    return t
