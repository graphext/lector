from __future__ import annotations

from typing import Dict, Iterable, Union

import pyarrow as pa
import pyarrow.csv as pacsv
from pyarrow import DataType
from pyarrow.csv import InvalidRow

from ..log import LOG, dict_view, schema_view
from ..utils import MISSING_STRINGS, ensure_type
from .abc import Reader

TypeDict = Dict[str, Union[str, DataType]]


class ArrowReader(Reader):
    """Use base class detection methods to configure a pyarrow.csv.read_csv() call."""

    def skip_invalid_row(self, row: InvalidRow) -> str:
        if row.text and len(row.text) > 100:
            row = row._replace(text=row.text[:100])

        print(f"Skipping row {row}")
        return "skip"

    def configure(self) -> None:
        self.config = {
            "read_options": {"encoding": self.encoding, "skip_rows": self.preamble},
            "parse_options": {
                "delimiter": self.dialect["delimiter"],
                "quote_char": self.dialect["quotechar"],
                "double_quote": self.dialect["doublequote"],
                "escape_char": self.dialect["escapechar"],
                "newlines_in_values": True,
                "invalid_row_handler": self.skip_invalid_row,
            },
            "convert_options": {
                "check_utf8": False,
                "strings_can_be_null": True,
                "quoted_strings_can_be_null": True,
            },
        }
        LOG.print(dict_view(self.config, title="Detected CSV format"))

    def parse(
        self,
        types: str | TypeDict | None = None,
        timestamp_formats: str | list[str] | None = None,
        null_values: str | Iterable[str] | None = None,
    ) -> pa.Table:

        ro = self.config["read_options"]
        po = self.config["parse_options"]
        co = self.config["convert_options"]

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

        tbl = pacsv.read_csv(
            self.fp,
            read_options=pa.csv.ReadOptions(**ro),
            parse_options=pa.csv.ParseOptions(**po),
            convert_options=pa.csv.ConvertOptions(**co),
        )

        LOG.print(schema_view(tbl.schema, title="Parsed table schema"))
        return tbl
