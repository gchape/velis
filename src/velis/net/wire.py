"""Field and message codecs for Velis's tagged binary wire format.

A message is a length-prefixed tag followed by its fields in declaration order.
Decoders read bytes or bytearray spans, reject trailing data and check field
limits before reading their bodies. Decoded values own their bytes.

A codec is built from one table of ``msg(tag, name=field, ...)`` entries and is
validated at import time: the table must cover exactly the message types of its
union alias, and each entry's field names, order and Python types must match
the dataclass it describes. Every field knows its largest encoded size, so the
codec also knows the largest body it can ever produce or accept.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from struct import Struct
from types import UnionType
from typing import Any, TypeAliasType, Union, get_args, get_origin, get_type_hints

from velis.errors import ProtocolError
from velis.net.limits import MAX_FIELD_BYTES, MAX_TAG_BYTES, MAX_UTF8_BYTES_PER_CHAR

_U32 = Struct(">I")
unpack_u32, pack_u32 = _U32.unpack_from, _U32.pack
_PREFIX_BYTES = _U32.size
_BAD_UTF8 = "invalid utf-8 in string field"

type Buffer = bytes | bytearray
type Reader[T] = Callable[[Buffer, int, int], tuple[T, int]]
type Writer[T] = Callable[[T], bytes]


@dataclass(frozen=True, slots=True)
class Field[T]:
    """How one value is read from and written to a message body.

    ``py`` is the dataclass annotation the field serves; ``max_size`` is the
    most bytes the field can occupy on the wire, length prefix included.
    """

    py: type[T]
    max_size: int
    read: Reader[T]
    write: Writer[T]


@dataclass(frozen=True, slots=True)
class Entry:
    """One message's tag and its named fields, in wire order."""

    tag: bytes
    schema: Mapping[str, Field[Any]]


def msg(tag: bytes, **schema: Field[Any]) -> Entry:
    """Declare a message: its tag, then one named field per dataclass attribute."""
    return Entry(tag, schema)


def _truncated(need: int) -> ProtocolError:
    return ProtocolError(f"truncated record: need {need} bytes")


def _decode_utf8(raw: Buffer) -> str:
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


def _span(data: Buffer, pos: int, end: int, limit: int) -> tuple[int, int]:
    """Locate a u32-length-prefixed field, checking its limit before its body."""
    if pos + _PREFIX_BYTES > end:
        raise _truncated(_PREFIX_BYTES)
    (length,) = unpack_u32(data, pos)
    if length > limit:
        raise ProtocolError(f"field too long: {length}")
    start = pos + _PREFIX_BYTES
    stop = start + length
    if stop > end:
        raise _truncated(length)
    return start, stop


def blob(limit: int) -> Field[bytes]:
    """Opaque bytes of at most ``limit`` bytes.

    Decoding copies only the retained field, without an intermediate mutable
    slice, and never retains a view of the input buffer.
    """

    def read(data: Buffer, pos: int, end: int) -> tuple[bytes, int]:
        start, stop = _span(data, pos, end, limit)
        if isinstance(data, bytes):
            return data[start:stop], stop
        with memoryview(data) as view:
            return bytes(view[start:stop]), stop

    def write(value: bytes) -> bytes:
        if len(value) > limit:
            raise ProtocolError(f"field too long: {len(value)}")
        return _prefixed(value)

    return Field(bytes, _PREFIX_BYTES + limit, read, write)


def free_text(limit: int) -> Field[str]:
    """UTF-8 text of at most ``limit`` encoded bytes, with no other restriction."""
    raw = blob(limit)

    def read(data: Buffer, pos: int, end: int) -> tuple[str, int]:
        value, stop = raw.read(data, pos, end)
        return _decode_utf8(value), stop

    def write(value: str) -> bytes:
        return raw.write(_encode_utf8(value))

    return Field(str, raw.max_size, read, write)


def text_tuple(max_items: int, max_bytes: int) -> Field[tuple[str, ...]]:
    """A counted tuple of UTF-8 strings, bounded before allocating its items."""
    item = free_text(max_bytes)

    def read(data: Buffer, pos: int, end: int) -> tuple[tuple[str, ...], int]:
        count, pos = U32.read(data, pos, end)
        if count > max_items:
            raise ProtocolError(f"too many text items: {count}")
        values: list[str] = []
        for _ in range(count):
            value, pos = item.read(data, pos, end)
            values.append(value)
        return tuple(values), pos

    def write(values: tuple[str, ...]) -> bytes:
        if type(values) is not tuple:
            raise ProtocolError("expected a tuple of strings")
        if len(values) > max_items:
            raise ProtocolError(f"too many text items: {len(values)}")
        if any(type(value) is not str for value in values):
            raise ProtocolError("expected a tuple of strings")
        return U32.write(len(values)) + b"".join(item.write(value) for value in values)

    return Field(
        tuple[str, ...], _PREFIX_BYTES + max_items * item.max_size, read, write
    )


def text(what: str, max_chars: int) -> Field[str]:
    """Non-empty UTF-8 text of at most max_chars characters."""
    limit = max_chars * MAX_UTF8_BYTES_PER_CHAR

    def check(value: str) -> str:
        if not value:
            raise ProtocolError(f"{what} must not be empty")
        if len(value) > max_chars:
            raise ProtocolError(f"{what} exceeds {max_chars} characters")
        return value

    def read(data: Buffer, pos: int, end: int) -> tuple[str, int]:
        start, stop = _span(data, pos, end, limit)
        return check(_decode_utf8(data[start:stop])), stop

    def write(value: str) -> bytes:
        return _prefixed(_encode_utf8(check(value)))

    return Field(str, _PREFIX_BYTES + limit, read, write)


def uint(layout: Struct) -> Field[int]:
    """Big-endian unsigned integer of the layout's width."""
    bits = layout.size * 8

    def read(data: Buffer, pos: int, end: int) -> tuple[int, int]:
        stop = pos + layout.size
        if stop > end:
            raise _truncated(layout.size)
        (value,) = layout.unpack_from(data, pos)
        return value, stop

    def write(value: int) -> bytes:
        if type(value) is not int or not 0 <= value < 1 << bits:
            raise ProtocolError(f"u{bits} integer out of range: {value}")
        return layout.pack(value)

    return Field(int, layout.size, read, write)


BLOB = blob(MAX_FIELD_BYTES)
FREE_TEXT = free_text(MAX_FIELD_BYTES)
U32 = uint(_U32)
U64 = uint(Struct(">Q"))


def _members(tp: object) -> set[object]:
    """Flatten ``type X = A | B | Y`` (Y itself an alias) into concrete classes."""
    if isinstance(tp, TypeAliasType):
        return _members(tp.__value__)
    if get_origin(tp) in (Union, UnionType):
        return {member for arg in get_args(tp) for member in _members(arg)}
    return {tp}


def _names(types: set[Any]) -> list[str]:
    return sorted(getattr(t, "__name__", repr(t)) for t in types)


class Codec[M]:
    """Encode and decode tagged dataclasses from one declaration table.

    ``covers`` is the union alias the table must serve. Construction fails if a
    member type is unregistered, an unknown type is registered, a tag is
    duplicated or too long, or an entry disagrees with its dataclass about
    field names, order or Python types. Unknown tags are rejected on decode.

    ``max_body`` is the largest body any registered message can occupy.
    """

    def __init__(
        self,
        kind: str,
        messages: Mapping[type[M], Entry],
        *,
        covers: TypeAliasType,
    ) -> None:
        declared = _members(covers)
        registered: set[object] = set(messages)
        if registered != declared:
            raise TypeError(
                f"{kind} table mismatch: unregistered {_names(declared - registered)}, "
                f"unexpected {_names(registered - declared)}"
            )
        self._kind = kind
        self._by_tag: dict[bytes, tuple[type[M], tuple[Field[Any], ...]]] = {}
        self._by_type: dict[
            type[M], tuple[bytes, tuple[tuple[str, Field[Any]], ...]]
        ] = {}
        self.max_body = 0
        for cls, entry in messages.items():
            self._register(cls, entry)

    def _register(self, cls: type[M], entry: Entry) -> None:
        if not is_dataclass(cls):
            raise TypeError(f"{cls.__name__} is not a dataclass")
        if len(entry.tag) > MAX_TAG_BYTES:
            raise ValueError(f"{cls.__name__}: tag longer than {MAX_TAG_BYTES} bytes")
        if entry.tag in self._by_tag:
            raise ValueError(f"duplicate {self._kind} tag: {entry.tag!r}")
        names = tuple(f.name for f in fields(cls))
        if tuple(entry.schema) != names:
            raise TypeError(
                f"{cls.__name__}: table declares {tuple(entry.schema)}, "
                f"dataclass has {names}"
            )
        hints = get_type_hints(cls)
        for name, field in entry.schema.items():
            if hints[name] != field.py:
                raise TypeError(
                    f"{cls.__name__}.{name}: annotated {hints[name]!r}, "
                    f"field serves {field.py!r}"
                )
        prefix = _prefixed(entry.tag)
        self._by_tag[entry.tag] = (cls, tuple(entry.schema.values()))
        self._by_type[cls] = (prefix, tuple(entry.schema.items()))
        size = len(prefix) + sum(f.max_size for f in entry.schema.values())
        self.max_body = max(self.max_body, size)

    def encode(self, value: M) -> bytes:
        """Encode a registered message, validating each field before returning."""
        entry = self._by_type.get(type(value))
        if entry is None:
            raise ProtocolError(f"unsupported {self._kind}: {type(value).__name__}")
        prefix, slots = entry
        if not slots:
            return prefix
        parts = [prefix]
        for name, field in slots:
            parts.append(field.write(getattr(value, name)))
        return b"".join(parts)

    def decode(self, data: Buffer, start: int = 0, end: int | None = None) -> M:
        """Decode one complete body, optionally inside a larger receive buffer.

        Fields cannot cross end, and no view of mutable input escapes this call.
        """
        if end is None:
            end = len(data)
        if not 0 <= start <= end <= len(data):
            raise ProtocolError("invalid record bounds")
        start, pos = _span(data, start, end, MAX_TAG_BYTES)
        tag = bytes(data[start:pos])
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
