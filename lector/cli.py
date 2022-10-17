"""Command-line interface."""
from pathlib import Path
from typing import Optional

import typer

from . import Inference, read_csv
from .log import LOG, pformat, schema_view, table_view
from .utils import Timer

CLI = typer.Typer()


@CLI.command()
def read(
    fp: Path = typer.Argument(..., exists=True, file_okay=True, dir_okay=False, resolve_path=True),
    types: Optional[Inference] = typer.Option(Inference.Auto),
    log: Optional[bool] = typer.Option(False),
):
    """Read a CSV file into an Arrow table."""
    with Timer() as t:
        tbl = read_csv(fp, types=types, log=log)

    LOG.info(pformat(table_view(tbl, title="Final table")))
    LOG.info(pformat(schema_view(tbl.schema, title="Schema")))
    LOG.info(f"Import took {t.elapsed:.2f} seconds.")
