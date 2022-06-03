"""Command-line interface."""
from pathlib import Path
from typing import Optional

import typer

from .arrow import ArrowReader

CLI = typer.Typer()


@CLI.command()
def read(
    fp: Path = typer.Argument(..., exists=True, file_okay=True, dir_okay=False, resolve_path=True),
    types: Optional[str] = typer.Option(None),
):
    """Read a CSV file into an Arrow table."""
    ArrowReader(fp).read(types=types)
