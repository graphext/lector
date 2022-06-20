"""Command-line interface."""
from pathlib import Path
from typing import Optional

import typer

from .csv import ArrowReader
from .log import LOG, schema_view, table_view
from .types import Autocast
from .utils import Timer

CLI = typer.Typer()


@CLI.command()
def read(
    fp: Path = typer.Argument(..., exists=True, file_okay=True, dir_okay=False, resolve_path=True),
    types: Optional[str] = typer.Option(None),
    autocast: Optional[bool] = typer.Option(False),
):
    """Read a CSV file into an Arrow table."""
    with Timer() as t:

        tbl = ArrowReader(fp).read(types=types)
        if autocast:
            tbl = Autocast(n_samples=100).cast(tbl)

    LOG.print(table_view(tbl, title="Final table"))
    LOG.print(schema_view(tbl.schema, title="Schema"))
    LOG.print(f"Import took {t.elapsed:.2f} seconds.")
