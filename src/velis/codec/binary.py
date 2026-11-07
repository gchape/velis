"""Reusable binary representations, independent of sockets."""

from dataclasses import dataclass

from velis.errors import ProtocolError


@dataclass(frozen=True, slots=True)
class UInt:
    bits: int

    def __post_init__(self) -> None:
        if self.bits <= 0 or self.bits % 8:
            raise ValueError("integer width must be a positive multiple of 8")

    @property
    def name(self) -> str:
        return f"u{self.bits}"

    @property
    def width(self) -> int:
        return self.bits // 8

    def encode(self, value: int) -> bytes:
        if not isinstance(value, int) or not 0 <= value < 1 << self.bits:
            raise ProtocolError(
                f"unsigned {self.bits}-bit integer out of range: {value}"
            )
        return value.to_bytes(self.width, "big")

    def decode(self, data: bytes) -> int:
        if len(data) != self.width:
            raise ProtocolError(f"expected {self.width} integer bytes")
        return int.from_bytes(data, "big")


U32 = UInt(32)
U64 = UInt(64)


@dataclass(frozen=True, slots=True)
class Utf8:
    name: str = "utf8"

    def encode(self, value: str) -> bytes:
        try:
            return value.encode("utf-8")
        except UnicodeEncodeError:
            raise ProtocolError("invalid utf-8 in string field") from None

    def decode(self, data: bytes) -> str:
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            raise ProtocolError("invalid utf-8 in string field") from None


@dataclass(frozen=True, slots=True)
class LengthPrefix:
    """One u32 length and exactly that many bytes, with a bounded body."""

    max_length: int
    name: str = "length"

    def __post_init__(self) -> None:
        if not 0 <= self.max_length < 1 << 32:
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
