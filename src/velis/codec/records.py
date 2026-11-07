"""Tagged records assembled from reversible field codecs."""

from dataclasses import dataclass
from typing import Any

from velis.codec.binary import U32
from velis.codec.chain import Codec
from velis.errors import ProtocolError


class _Cursor:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def take(self, size: int) -> bytes:
        end = self.offset + size
        if end > len(self.data):
            raise ProtocolError("truncated record")
        data = self.data[self.offset : end]
        self.offset = end
        return data

    def prefixed(self) -> bytes:
        """Take one complete field, including its u32 length header."""
        header = self.take(U32.width)
        return header + self.take(U32.decode(header))


@dataclass(frozen=True, slots=True)
class Field[T]:
    """A named field encoded with fixed width or a u32 length prefix.

    The codec owns field validation. For variable-width fields, it receives
    the length prefix as well as the body when decoding.
    """

    name: str
    codec: Codec[T, bytes]
    width: int | None = None

    def __post_init__(self) -> None:
        if self.width is not None and self.width <= 0:
            raise ValueError("field width must be positive")

    def read(self, cursor: _Cursor) -> T:
        if self.width is not None:
            return self.codec.decode(cursor.take(self.width))
        return self.codec.decode(cursor.prefixed())


@dataclass(frozen=True, slots=True)
class Record[T]:
    """A tagged record constructed by field name, in the declared wire order."""

    kind: type[T]
    tag: str
    fields: tuple[Field[Any], ...] = ()

    def __post_init__(self) -> None:
        if len({field.name for field in self.fields}) != len(self.fields):
            raise ValueError("duplicate field name")


@dataclass(frozen=True, slots=True)
class TaggedCodec[T]:
    name: str
    tag_codec: Codec[str, bytes]
    records: tuple[Record[T], ...]

    def __post_init__(self) -> None:
        if len({record.tag for record in self.records}) != len(self.records):
            raise ValueError("duplicate record tag")
        if len({record.kind for record in self.records}) != len(self.records):
            raise ValueError("duplicate record type")

    def encode(self, value: T) -> bytes:
        for record in self.records:
            if type(value) is record.kind:
                return self.tag_codec.encode(record.tag) + b"".join(
                    field.codec.encode(getattr(value, field.name))
                    for field in record.fields
                )
        raise ProtocolError(f"unknown {self.name} type: {type(value).__name__}")

    def decode(self, data: bytes) -> T:
        cursor = _Cursor(data)
        tag = self.tag_codec.decode(cursor.prefixed())
        for record in self.records:
            if record.tag == tag:
                values = {field.name: field.read(cursor) for field in record.fields}
                if cursor.offset != len(data):
                    raise ProtocolError("trailing bytes after record")
                return record.kind(**values)
        raise ProtocolError(f"unknown {self.name}: {tag!r}")
