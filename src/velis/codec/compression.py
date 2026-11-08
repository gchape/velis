"""Stateless, bounded compression for payload and wire codec chains."""

import sys
import zlib
from dataclasses import dataclass

from velis.errors import ProtocolError


@dataclass(frozen=True, slots=True)
class Deflate:
    """Encode one zlib-wrapped DEFLATE stream per value.

    max_length bounds uncompressed input and decompressed output. Compressed
    bytes may be larger; outer field/frame codecs enforce their own limits.
    Each call uses independent state, so instances can be shared by peers.
    """

    level: int = -1
    max_length: int = 1 << 20

    def __post_init__(self) -> None:
        if not -1 <= self.level <= 9:
            raise ValueError("compression level must be between -1 and 9")
        if not 0 <= self.max_length < sys.maxsize:
            raise ValueError(
                "uncompressed limit must be nonnegative and below sys.maxsize"
            )

    def encode(self, data: bytes) -> bytes:
        if len(data) > self.max_length:
            raise ProtocolError("uncompressed data exceeds limit")
        try:
            return zlib.compress(data, self.level)
        except zlib.error:
            raise ProtocolError("compression failed") from None

    def decode(self, data: bytes) -> bytes:
        """Decode one complete stream, allowing one extra byte to detect overflow."""
        decoder = zlib.decompressobj()
        try:
            decoded = decoder.decompress(data, self.max_length + 1)
        except zlib.error:
            raise ProtocolError("invalid compressed data") from None
        if len(decoded) > self.max_length:
            raise ProtocolError("uncompressed data exceeds limit")
        if not decoder.eof:
            raise ProtocolError("incomplete compressed stream")
        if decoder.unused_data or decoder.unconsumed_tail:
            raise ProtocolError("trailing bytes after compressed stream")
        return decoded
