"""Explicit record boundaries around reversible value codecs."""

from dataclasses import dataclass, field
from typing import Protocol

from velis.codec.binary import U32, LengthPrefix
from velis.codec.chain import Codec
from velis.codec.cursor import Cursor
from velis.errors import ProtocolError


class Field[T](Codec[T, bytes], Protocol):
    """A codec that also knows how to consume one field from a record."""

    def read(self, cursor: Cursor) -> T: ...


@dataclass(frozen=True, slots=True)
class Fixed[T]:
    """Encode or consume exactly width bytes around a value codec."""

    codec: Codec[T, bytes]
    width: int

    def __post_init__(self) -> None:
        if self.width <= 0:
            raise ValueError("field width must be positive")

    def _check(self, data: bytes) -> None:
        if len(data) != self.width:
            raise ProtocolError(f"expected {self.width} field bytes")

    def encode(self, value: T) -> bytes:
        data = self.codec.encode(value)
        self._check(data)
        return data

    def decode(self, data: bytes) -> T:
        self._check(data)
        return self.codec.decode(data)

    def read(self, cursor: Cursor) -> T:
        return self.codec.decode(cursor.take(self.width))


@dataclass(frozen=True, slots=True)
class Prefixed[T]:
    """Bound a value's encoded bytes with a u32 prefix checked before reading."""

    codec: Codec[T, bytes]
    max_length: int
    _framing: LengthPrefix = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_framing", LengthPrefix(self.max_length))

    def encode(self, value: T) -> bytes:
        return self._framing.encode(self.codec.encode(value))

    def decode(self, data: bytes) -> T:
        return self.codec.decode(self._framing.decode(data))

    def read(self, cursor: Cursor) -> T:
        length = self._framing.decode_length(cursor.take(U32.width))
        return self.codec.decode(cursor.take(length))
