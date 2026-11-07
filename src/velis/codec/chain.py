from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class Codec(Protocol):
    """Named transformation with reversible bytes-to-bytes operations."""

    @property
    def name(self) -> str:
        """Identify the transformation for chain naming and registration."""
        ...

    def encode(self, data: bytes) -> bytes:
        """Transform bytes into the encoded representation."""
        ...

    def decode(self, data: bytes) -> bytes:
        """Restore bytes from the encoded representation."""
        ...


@dataclass(frozen=True, slots=True)
class CodecChain:
    """Compose codecs in order, decoding in reverse order.

    An empty chain preserves bytes unchanged. Codec errors propagate
    to the caller.
    """

    codecs: tuple[Codec, ...] = ()

    @classmethod
    def of(cls, *codecs: Codec) -> CodecChain:
        """Build a chain from codecs in their encoding order."""
        return cls(codecs)

    @property
    def name(self) -> str:
        """Join codec names with underscores in encoding order."""
        return "_".join(codec.name for codec in self.codecs)

    def encode(self, data: bytes) -> bytes:
        """Apply each codec from left to right."""
        for codec in self.codecs:
            data = codec.encode(data)
        return data

    def decode(self, data: bytes) -> bytes:
        """Apply each decoder from right to left."""
        for codec in reversed(self.codecs):
            data = codec.decode(data)
        return data

    def __or__(self, other: Codec | CodecChain) -> CodecChain:
        """Append a codec or flatten another chain into a new chain."""
        appended = other.codecs if isinstance(other, CodecChain) else (other,)
        return CodecChain(self.codecs + appended)

    def __ror__(self, other: Codec) -> CodecChain:
        """Prepend a codec without changing this chain."""
        return CodecChain((other, *self.codecs))
