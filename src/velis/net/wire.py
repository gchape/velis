"""Field and message codecs for Velis's tagged binary wire format.

A message is a length-prefixed tag followed by its fields in declaration order.
Decoders require bytes, reject trailing data and check field limits before
reading or decoding their bodies.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from struct import Struct
from typing import Any

from velis.errors import ProtocolError
from velis.net.limits import MAX_FIELD_BYTES, MAX_TAG_BYTES

_U32 = Struct(">I")
unpack_u32, pack_u32 = _U32.unpack_from, _U32.pack
_BAD_UTF8 = "invalid utf-8 in string field"

type Reader[T] = Callable[[bytes, int, int], tuple[T, int]]
type Writer[T] = Callable[[T], bytes]


@dataclass(frozen=True, slots=True)
class Field[T]:
    """How one value is read from and written to a message body."""

    read: Reader[T]
    write: Writer[T]


def _truncated(need: int) -> ProtocolError:
    return ProtocolError(f"truncated record: need {need} bytes")


def _decode_utf8(raw: bytes) -> str:
    try:
        return raw.decode()
    except UnicodeError as exc:
        raise ProtocolError(_BAD_UTF8) from exc


def _encode_utf8(value: str) -> bytes:
    try:
        return value.encode()
    except UnicodeError as exc:
        raise ProtocolError(_BAD_UTF8) from exc


def _prefixed(raw: bytes) -> bytes:
    return pack_u32(len(raw)) + raw


def _span(data: bytes, pos: int, end: int, limit: int) -> tuple[int, int]:
    """Locate a u32-length-prefixed field, checking its limit before its body."""
    if pos + 4 > end:
        raise _truncated(4)
    (length,) = unpack_u32(data, pos)
    if length > limit:
        raise ProtocolError(f"field too long: {length}")
    start = pos + 4
    stop = start + length
    if stop > end:
        raise _truncated(length)
    return start, stop


def _read_blob(data: bytes, pos: int, end: int) -> tuple[bytes, int]:
    start, stop = _span(data, pos, end, MAX_FIELD_BYTES)
    return data[start:stop], stop


def _write_blob(value: bytes) -> bytes:
    if len(value) > MAX_FIELD_BYTES:
        raise ProtocolError(f"field too long: {len(value)}")
    return _prefixed(value)


def _read_free_text(data: bytes, pos: int, end: int) -> tuple[str, int]:
    raw, stop = _read_blob(data, pos, end)
    return _decode_utf8(raw), stop


def _write_free_text(value: str) -> bytes:
    return _write_blob(_encode_utf8(value))


BLOB = Field(_read_blob, _write_blob)
FREE_TEXT = Field(_read_free_text, _write_free_text)


def text(what: str, max_chars: int) -> Field[str]:
    """Non-empty UTF-8 text of at most max_chars characters."""

    def check(value: str) -> str:
        if not value:
            raise ProtocolError(f"{what} must not be empty")
        if len(value) > max_chars:
            raise ProtocolError(f"{what} exceeds {max_chars} characters")
        return value

    def read(data: bytes, pos: int, end: int) -> tuple[str, int]:
        start, stop = _span(data, pos, end, max_chars * 4)
        return check(_decode_utf8(data[start:stop])), stop

    def write(value: str) -> bytes:
        return _write_blob(_encode_utf8(check(value)))

    return Field(read, write)


def uint(layout: Struct) -> Field[int]:
    """Big-endian unsigned integer of the layout's width."""
    bits = layout.size * 8

    def read(data: bytes, pos: int, end: int) -> tuple[int, int]:
        stop = pos + layout.size
        if stop > end:
            raise _truncated(layout.size)
        (value,) = layout.unpack_from(data, pos)
        return value, stop

    def write(value: int) -> bytes:
        if not 0 <= value < 1 << bits:
            raise ProtocolError(f"u{bits} integer out of range: {value}")
        return layout.pack(value)

    return Field(read, write)


U32 = uint(_U32)
U64 = uint(Struct(">Q"))


class Codec[M]:
    """Encode and decode tagged dataclasses from one declaration table.

    Each entry maps a dataclass to its tag and one field per dataclass
    attribute, in order. Unregistered types and unknown tags are rejected.
    """

    def __init__(
        self,
        kind: str,
        messages: Mapping[type[M], tuple[bytes, *tuple[Field[Any], ...]]],
    ) -> None:
        self._kind = kind
        self._by_tag: dict[bytes, tuple[type[M], tuple[Field[Any], ...]]] = {}
        self._by_type: dict[
            type[M], tuple[bytes, tuple[tuple[str, Field[Any]], ...]]
        ] = {}
        for cls, (tag, *schema) in messages.items():
            if not is_dataclass(cls):
                raise TypeError(f"{cls.__name__} is not a dataclass")
            names = tuple(f.name for f in fields(cls))
            if len(names) != len(schema):
                raise TypeError(
                    f"{cls.__name__} has {len(names)} attributes, {len(schema)} fields"
                )
            if tag in self._by_tag:
                raise ValueError(f"duplicate {kind} tag: {tag!r}")
            self._by_tag[tag] = (cls, tuple(schema))
            self._by_type[cls] = (
                _prefixed(tag),
                tuple(zip(names, schema, strict=True)),
            )

    def encode(self, value: M) -> bytes:
        entry = self._by_type.get(type(value))
        if entry is None:
            raise ProtocolError(f"unsupported {self._kind}: {type(value).__name__}")
        prefix, slots = entry
        return prefix + b"".join(
            field.write(getattr(value, name)) for name, field in slots
        )

    def decode(self, data: bytes) -> M:
        end = len(data)
        start, pos = _span(data, 0, end, MAX_TAG_BYTES)
        tag = data[start:pos]
        entry = self._by_tag.get(tag)
        if entry is None:
            raise ProtocolError(f"unknown {self._kind}: {_decode_utf8(tag)!r}")
        cls, schema = entry
        values: list[Any] = []
        for field in schema:
            value, pos = field.read(data, pos, end)
            values.append(value)
        if pos != end:
            raise ProtocolError("trailing bytes after record")
        return cls(*values)
