"""Reusable binary representations, independent of sockets."""

from dataclasses import dataclass

from velis.errors import ProtocolError


@dataclass(frozen=True, slots=True)
class Identity:
    def encode(self, value: bytes) -> bytes:
        return value

    def decode(self, data: bytes) -> bytes:
        return data


@dataclass(frozen=True, slots=True)
class UInt:
    """Big-endian unsigned integer of a fixed width in bytes."""

    width: int

    def __post_init__(self) -> None:
        if self.width <= 0:
            raise ValueError("integer width must be positive")

    def encode(self, value: int) -> bytes:
        try:
            return value.to_bytes(self.width)
        except OverflowError:
            raise ProtocolError(
                f"u{self.width * 8} integer out of range: {value}"
            ) from None

    def decode(self, data: bytes) -> int:
        if len(data) != self.width:
            raise ProtocolError(f"expected {self.width} integer bytes")
        return int.from_bytes(data)


U32 = UInt(4)
U64 = UInt(8)


@dataclass(frozen=True, slots=True)
class Utf8:
    def encode(self, value: str) -> bytes:
        try:
            return value.encode()
        except UnicodeEncodeError:
            raise ProtocolError("invalid utf-8 in string field") from None

    def decode(self, data: bytes) -> str:
        try:
            return data.decode()
        except UnicodeDecodeError:
            raise ProtocolError("invalid utf-8 in string field") from None


@dataclass(frozen=True, slots=True)
class LengthPrefix:
    """One u32 length and exactly that many bytes, with a bounded body."""

    max_length: int

    def __post_init__(self) -> None:
        if not 0 <= self.max_length < 1 << (U32.width * 8):
            raise ValueError("length limit must fit an unsigned 32-bit integer")

    def check_length(self, length: int) -> None:
        if not 0 <= length <= self.max_length:
            raise ProtocolError(f"field too long: {length}")

    def encode(self, data: bytes) -> bytes:
        self.check_length(len(data))
        return U32.encode(len(data)) + data

    def decode_length(self, header: bytes) -> int:
        """Validate a header before a stream adapter reads its body."""
        length = U32.decode(header)
        self.check_length(length)
        return length

    def decode(self, data: bytes) -> bytes:
        length = self.decode_length(data[: U32.width])
        if len(data) != length + U32.width:
            raise ProtocolError("length prefix does not match body")
        return data[U32.width :]
