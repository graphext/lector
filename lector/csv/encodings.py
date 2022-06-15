"""Helpers to detecting character encodings in binary buffers."""
from __future__ import annotations

import codecs
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import BinaryIO, Literal

import cchardet as cdet

BOMS: dict[str, tuple[Literal, ...]] = {
    "utf-8-sig": (codecs.BOM_UTF8,),
    "utf-16": (codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE),
    "utf-32": (codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE),
}
"""Map BOM (Byte-order mark) to encoding."""


MAX_INT32: int = 2_147_483_647
"""Cannot read more than this number of bytes at once to detect encoding."""

CODEC_ERR_CHAR = "�"
"""Character representing non-codable bytes."""


def detect_bom(bs: bytes):
    """Detect encoding by looking for a BOM at the start of the file."""
    for enc, boms in BOMS.items():
        if any(bs.startswith(bom) for bom in boms):
            return enc

    return None


def prop_decoding_errors(bs: bytes, encoding: str) -> float:
    """The proportion of characters that couldn't be decoded correctly."""
    string = bytes.decode(bs, encoding, errors="replace")
    n_err = string.count(CODEC_ERR_CHAR)
    return n_err / len(string)


@dataclass
class EncodingDetector(ABC):
    """Base class specifying interface for all encoding detetors."""

    @abstractmethod
    def detect(self, buffer: BinaryIO) -> str:
        """Implement me."""


@dataclass
class Chardet(EncodingDetector):
    """An encoding detector using cchardet if the default utf-8 generates too many errors."""

    n_bytes: int = int(1e7)  # 10 MB
    """Use this many bytes to detect encoding."""
    error_threshold: float = 0.05
    """A greater proportion of decoding errors than this will be considered a failed encoding."""
    confidence_threshold: float = 0.6
    """Minimum level of confidence to accept an encoding automatically detected by cchardet."""

    def detect(self, buffer: BinaryIO) -> str:
        """Somewhat 'opinionated' encoding detection.

        Assumes utf-8 as most common encoding, falling back on cchardet detection, and
        if all else fails on windows-1250 if encoding is latin-like.
        """
        head: bytes = buffer.read(min(self.n_bytes, MAX_INT32))

        bom_encoding = detect_bom(head)
        if bom_encoding:
            return bom_encoding

        if prop_decoding_errors(head, "utf-8") <= self.error_threshold:
            return "utf-8"

        detected = cdet.detect(head)
        encoding, confidence = detected["encoding"], detected["confidence"]

        if encoding:
            if confidence > self.confidence_threshold:
                return encoding
            else:
                if any(label in encoding.lower() for label in ("windows", "iso-8859")):
                    # Iso-like, will use windows-1250 as super set for special chars
                    return "windows-1250"

        return "windows-1250"
