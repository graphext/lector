from __future__ import annotations

from io import BytesIO, TextIOBase
from typing import Dict, Iterable, Union

import pyarrow as pa
import pyarrow.csv as pacsv
from pyarrow import DataType
from pyarrow.csv import InvalidRow

from ..log import LOG
from ..utils import MISSING_STRINGS, ensure_type
from .abc import EmptyFileError, Format, Reader

TypeDict = Dict[str, Union[str, DataType]]


class ArrowReader(Reader):
    """Use base class detection methods to configure a pyarrow.csv.read_csv() call."""

    @staticmethod
    def skip_invalid_row(row: InvalidRow) -> str:
        if row.text and len(row.text) > 100:
            row = row._replace(text=row.text[:100])

        LOG.print(f"Skipping row {row}")
        return "skip"

    @classmethod
    def configure(cls, format: Format) -> dict:
        return {
            "read_options": {
                "encoding": format.encoding,
                "skip_rows": format.preamble,
            },
            "parse_options": {
                "delimiter": format.dialect.delimiter,
                "quote_char": format.dialect.quote_char,
                "double_quote": format.dialect.double_quote,
                "escape_char": format.dialect.escape_char,
                "newlines_in_values": True,
                "invalid_row_handler": cls.skip_invalid_row,
            },
            "convert_options": {
                "check_utf8": False,
                "strings_can_be_null": True,
                "quoted_strings_can_be_null": True,
            },
        }

    def parse(
        self,
        types: str | TypeDict | None = None,
        timestamp_formats: str | list[str] | None = None,
        null_values: str | Iterable[str] | None = None,
    ) -> pa.Table:
        """Invoke Arrow's parser with inferred CSV format."""

        config = self.configure(self.format)

        ro = config["read_options"]
        po = config["parse_options"]
        co = config["convert_options"]

        if types is not None:

            if isinstance(types, (str, DataType)):
                types = {col: ensure_type(types) for col in self.columns}
            elif isinstance(types, dict):
                types = {col: ensure_type(type) for col, type in types.items()}

            co["column_types"] = types

        if timestamp_formats is not None:

            if not isinstance(timestamp_formats, list):
                timestamp_formats = [timestamp_formats]

            co["timestamp_parsers"] = timestamp_formats

        if null_values is not None:

            if isinstance(null_values, str):
                null_values = [null_values]
            else:
                null_values = list(null_values)

            co["null_values"] = null_values
        else:
            co["null_values"] = MISSING_STRINGS

        try:
            fp = self.fp
            if isinstance(fp, TextIOBase):
                fp = BytesIO(fp.read().encode("utf-8"))

            tbl = pacsv.read_csv(
                fp,
                read_options=pa.csv.ReadOptions(**ro),
                parse_options=pa.csv.ParseOptions(**po),
                convert_options=pa.csv.ConvertOptions(**co),
            )
        except pa.ArrowInvalid as exc:

            if "Empty CSV file or block" in (msg := str(exc)):
                raise EmptyFileError(msg)

            raise

        # Arrow doesn't (yet?) have support for CSV dialect "skipinitialspace" option
        # At least do minimal clean up of column names
        tbl = tbl.rename_columns([name.strip() for name in tbl.column_names])

        return tbl
