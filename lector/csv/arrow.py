from __future__ import annotations

import codecs
from codecs import StreamRecoder
from collections.abc import Iterable
from io import SEEK_CUR, BufferedIOBase, TextIOBase
from pathlib import Path
from typing import Union

import pyarrow as pa
import pyarrow.csv as pacsv
from pyarrow import DataType
from pyarrow.csv import InvalidRow

from ..log import LOG
from ..utils import MISSING_STRINGS, ensure_type, uniquify
from .abc import EmptyFileError, FileLike, Format, Reader

TypeDict = dict[str, Union[str, DataType]]

MAX_MSG_LEN = 200  # characters
SKIPPED_MSG_N_MAX = 20


def clean_column_names(names: list[str]) -> list[str]:
    """Handle empty and duplicate column names."""

    # Arrow doesn't (yet?) have support for CSV dialect "skipinitialspace" option
    names = [name.strip() for name in names]
    unnamed = [i for i, x in enumerate(names) if not x]
    for i, col_idx in enumerate(unnamed):
        names[col_idx] = f"Unnamed_{i}"

    return uniquify(names)


def transcode(
    fp: FileLike,
    codec_in: str = "utf-8",
    codec_out: str = "utf-8",
    errors="replace",
) -> StreamRecoder:
    """Safely transcode any readable byte stream from decoder to encoder codecs.

    Arrow only accepts byte streams and optional encoding, but has no option to
    automatically handle codec errors. It also doesn't seem to like the interface
    of a Python recoder when the encoding is "utf-16" (rather than more specific
    "utf-16-le" or "utf-16-be").
    """
    if isinstance(fp, (str, Path)):
        fp = open(fp, "rb")  # noqa: SIM115
    elif isinstance(fp, TextIOBase):
        # Not a no-operation! If we read 3 characteres from a text buffer, the underlying binary
        # buffer might actually read more, since it reads in batches. Which means its internal
        # cursor might be in advance of the current position in the text buffer read so far.
        fp.seek(0, SEEK_CUR)
        fp = fp.buffer

    if not isinstance(fp, BufferedIOBase):
        raise ValueError(f"Have unsupported input: {type(fp)}")

    return codecs.EncodedFile(fp, data_encoding=codec_out, file_encoding=codec_in, errors=errors)


class ArrowReader(Reader):
    """Use base class detection methods to configure a pyarrow.csv.read_csv() call."""

    def skip_invalid_row(self, row: InvalidRow) -> str:
        self.n_skipped += 1

        if self.n_skipped < SKIPPED_MSG_N_MAX:
            if row.text and len(row.text) > MAX_MSG_LEN:
                row = row._replace(text=row.text[:MAX_MSG_LEN])
                LOG.warning(f"Skipping row:\n{row}")

        elif self.n_skipped == SKIPPED_MSG_N_MAX:
            LOG.warning("Won't show more skipped row messages.")

        return "skip"

    def configure(self, format: Format) -> dict:
        return {
            "read_options": {
                "encoding": format.encoding,
                "skip_rows": format.preamble,
                "block_size": 2 << 20,  # 2 MiB, twice arrow's default of 1 MiB (1 << 20)
            },
            "parse_options": {
                "delimiter": format.dialect.delimiter,
                "quote_char": format.dialect.quote_char,
                "double_quote": format.dialect.double_quote,
                "escape_char": format.dialect.escape_char,
                "newlines_in_values": True,
                "invalid_row_handler": self.skip_invalid_row,
            },
            "convert_options": {
                "check_utf8": False,
                "strings_can_be_null": True,
                "quoted_strings_can_be_null": True,
            },
        }

    def parse(  # noqa: PLR0912
        self,
        types: str | TypeDict | None = None,
        timestamp_formats: str | list[str] | None = None,
        null_values: str | Iterable[str] | None = None,
    ) -> pa.Table:
        """Invoke Arrow's parser with inferred CSV format."""
        self.n_skipped = 0

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
            fp = transcode(self.fp, codec_in=self.encoding, codec_out="utf-8")
            ro["encoding"] = "utf-8"

            tbl = pacsv.read_csv(
                fp,
                read_options=pa.csv.ReadOptions(**ro),
                parse_options=pa.csv.ParseOptions(**po),
                convert_options=pa.csv.ConvertOptions(**co),
            )

            column_names = list(clean_column_names(tbl.column_names))
            tbl = tbl.rename_columns(column_names)
            return tbl
        except pa.ArrowInvalid as exc:
            if "Empty CSV file or block" in (msg := str(exc)):
                raise EmptyFileError(msg) from None

            raise
