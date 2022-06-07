"""Command-line interface."""
from pathlib import Path
from typing import Optional

import typer

from .csv import ArrowReader
from .log import LOG, schema_view
from .types import Autocast

CLI = typer.Typer()


@CLI.command()
def read(
    fp: Path = typer.Argument(..., exists=True, file_okay=True, dir_okay=False, resolve_path=True),
    types: Optional[str] = typer.Option(None),
    autocast: Optional[bool] = typer.Option(False),
):
    """Read a CSV file into an Arrow table."""
    tbl = ArrowReader(fp).read(types=types)

    if autocast:
        tbl = Autocast().cast(tbl)

    LOG.print(schema_view(tbl.schema, title="Cast table schema"))
