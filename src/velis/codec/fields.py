"""Cursor-readable fields with explicit byte boundaries."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Protocol

from velis.codec.base import Codec
from velis.codec.cursor import Cursor
from velis.codec.values import UInt
from velis.errors import ProtocolError
from velis.validation import require_length


class Field[T](Codec[T, bytes], Protocol):
    """A codec that can read one complete field from a record cursor."""

    def read(self, cursor: Cursor) -> T: ...


class CursorCodec[T](ABC):
    """Decode one cursor-readable value and require complete consumption."""

    __slots__ = ()

    @abstractmethod
    def read(self, cursor: Cursor) -> T: ...

    def decode(self, data: bytes) -> T:
        with Cursor(data) as cursor:
            return self.read(cursor)


@dataclass(frozen=True, slots=True)
class FixedLengthField[T]:
    """A field of exactly length bytes, interpreted by the inner value codec."""

    codec: Codec[T, bytes]
    length: int

    def __post_init__(self) -> None:
        if self.length <= 0:
            raise ValueError("field length must be positive")

    def encode(self, value: T) -> bytes:
        data = self.codec.encode(value)
        require_length(data, self.length, what="field")
        return data

    def decode(self, data: bytes) -> T:
        require_length(data, self.length, what="field")
        return self.codec.decode(data)

    def read(self, cursor: Cursor) -> T:
        return self.codec.decode(cursor.take(self.length))


@dataclass(frozen=True, slots=True)
class LengthPrefixedField[T](CursorCodec[T]):
    """A field whose encoded byte length precedes its encoded value.

    prefix explicitly selects the unsigned integer codec for the length.
    max_length bounds the encoded value in bytes, excluding the prefix.
    Check this limit before consuming and decoding the field body.
    """

    codec: Codec[T, bytes]
    max_length: int
    prefix: UInt = field(kw_only=True)

    def __post_init__(self) -> None:
        if not 0 <= self.max_length <= self.prefix.max_value:
            raise ValueError("length limit must fit the prefix codec")

    def check_length(self, length: int) -> None:
        """Enforce the encoded body's byte limit."""
        if length > self.max_length:
            raise ProtocolError(f"field too long: {length}")

    def encode(self, value: T) -> bytes:
        data = self.codec.encode(value)
        self.check_length(len(data))
        return self.prefix.encode(len(data)) + data

    def read(self, cursor: Cursor) -> T:
        length = self.prefix.read(cursor)
        self.check_length(length)
        return self.codec.decode(cursor.take(length))
