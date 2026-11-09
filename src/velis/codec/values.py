"""Reversible value representations and validation, independent of cursors."""

from collections.abc import Callable
from dataclasses import dataclass

from velis.errors import ProtocolError

_BAD_UTF8 = "invalid utf-8 in string field"


@dataclass(frozen=True, slots=True)
class Identity:
    def encode(self, value: bytes) -> bytes:
        return value

    def decode(self, data: bytes) -> bytes:
        return data


@dataclass(frozen=True, slots=True)
class UInt:
    """Big-endian unsigned integer of a fixed length in bytes."""

    length: int

    def __post_init__(self) -> None:
        if self.length <= 0:
            raise ValueError("integer length must be positive")

    @property
    def max_value(self) -> int:
        return 256**self.length - 1

    def encode(self, value: int) -> bytes:
        if not 0 <= value <= self.max_value:
            raise ProtocolError(f"u{self.length * 8} integer out of range: {value}")
        return value.to_bytes(self.length)

    def decode(self, data: bytes) -> int:
        if len(data) != self.length:
            raise ProtocolError(f"expected {self.length} integer bytes")
        return int.from_bytes(data)


U32 = UInt(4)


U64 = UInt(8)


@dataclass(frozen=True, slots=True)
class UTF8:
    def encode(self, value: str) -> bytes:
        try:
            return value.encode()
        except UnicodeError as exc:
            raise ProtocolError(_BAD_UTF8) from exc

    def decode(self, data: bytes) -> str:
        try:
            return data.decode()
        except UnicodeError as exc:
            raise ProtocolError(_BAD_UTF8) from exc


@dataclass(frozen=True, slots=True)
class Validate[T]:
    """Preserve a value unless its rule returns an error message.

    Return None to accept the value; a message raises ProtocolError in either
    direction. Place the rule at the representation it validates.
    """

    rule: Callable[[T], str | None]

    def encode(self, value: T) -> T:
        if (problem := self.rule(value)) is not None:
            raise ProtocolError(problem)
        return value

    decode = encode
