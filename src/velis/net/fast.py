"""Offset-based codecs for Velis's fixed command and server-message bodies.

Decoders require bytes, reject trailing data and check field limits before
reading or decoding their bodies. Tags and field order are fixed by the protocol.
"""

from collections.abc import Callable, Mapping
from struct import Struct

from velis.core.messages import (
    NIL,
    OK,
    PERSISTENT,
    TTL,
    Command,
    Delete,
    Delivered,
    Err,
    Expire,
    Get,
    Message,
    Nil,
    Ok,
    Persist,
    Persistent,
    Publish,
    Reply,
    Set,
    Subscribe,
    Subscribed,
    TTLValue,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.errors import ProtocolError
from velis.net.limits import (
    MAX_CHANNEL_CHARS,
    MAX_FIELD_BYTES,
    MAX_KEY_CHARS,
    MAX_TAG_BYTES,
)

_U32 = Struct(">I")
_U64 = Struct(">Q")
unpack_u32, pack_u32 = _U32.unpack_from, _U32.pack
_BAD_UTF8 = "invalid utf-8 in string field"

type ServerFrame = Reply | Message
type _Decoder[T] = Callable[[bytes, int, int], T]
type _Reader[T] = Callable[[bytes, int, int], tuple[T, int]]


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


def _text(
    data: bytes, pos: int, end: int, what: str, max_chars: int
) -> tuple[str, int]:
    start, stop = _span(data, pos, end, max_chars * 4)
    text = _decode_utf8(data[start:stop])
    if not text:
        raise ProtocolError(f"{what} must not be empty")
    if len(text) > max_chars:
        raise ProtocolError(f"{what} exceeds {max_chars} characters")
    return text, stop


def _free_text(data: bytes, pos: int, end: int) -> tuple[str, int]:
    start, stop = _span(data, pos, end, MAX_FIELD_BYTES)
    return _decode_utf8(data[start:stop]), stop


def _bytes(data: bytes, pos: int, end: int) -> tuple[bytes, int]:
    start, stop = _span(data, pos, end, MAX_FIELD_BYTES)
    return data[start:stop], stop


def _number(data: bytes, pos: int, end: int, integer: Struct) -> tuple[int, int]:
    stop = pos + integer.size
    if stop > end:
        raise _truncated(integer.size)
    (value,) = integer.unpack_from(data, pos)
    return value, stop


def _read_key(data: bytes, pos: int, end: int) -> tuple[str, int]:
    return _text(data, pos, end, "key", MAX_KEY_CHARS)


def _read_channel(data: bytes, pos: int, end: int) -> tuple[str, int]:
    return _text(data, pos, end, "channel", MAX_CHANNEL_CHARS)


def _read_u32(data: bytes, pos: int, end: int) -> tuple[int, int]:
    return _number(data, pos, end, _U32)


def _read_u64(data: bytes, pos: int, end: int) -> tuple[int, int]:
    return _number(data, pos, end, _U64)


def _finish(pos: int, end: int) -> None:
    if pos != end:
        raise ProtocolError("trailing bytes after record")


def _one[T, R](cls: Callable[[T], R], read: _Reader[T]) -> _Decoder[R]:
    def decode(data: bytes, pos: int, end: int) -> R:
        value, pos = read(data, pos, end)
        _finish(pos, end)
        return cls(value)

    return decode


def _pair[A, B, R](
    cls: Callable[[A, B], R], first: _Reader[A], second: _Reader[B]
) -> _Decoder[R]:
    def decode(data: bytes, pos: int, end: int) -> R:
        a, pos = first(data, pos, end)
        b, pos = second(data, pos, end)
        _finish(pos, end)
        return cls(a, b)

    return decode


def _empty[T](value: T) -> _Decoder[T]:
    def decode(data: bytes, pos: int, end: int) -> T:
        _finish(pos, end)
        return value

    return decode


def _dispatch[T](decoders: Mapping[bytes, _Decoder[T]], data: bytes, kind: str) -> T:
    end = len(data)
    start, stop = _span(data, 0, end, MAX_TAG_BYTES)
    tag = data[start:stop]
    decoder = decoders.get(tag)
    if decoder is None:
        name = _decode_utf8(tag)
        raise ProtocolError(f"unknown {kind}: {name!r}")
    return decoder(data, stop, end)


_PREFIXES = {
    name: pack_u32(len(name)) + name
    for name in (
        b"GET",
        b"SET",
        b"EXPIRE",
        b"DEL",
        b"TTL",
        b"PERSIST",
        b"SUBSCRIBE",
        b"UNSUBSCRIBE",
        b"PUBLISH",
        b"OK",
        b"NIL",
        b"PERSISTENT",
        b"VALUE",
        b"ERR",
        b"SUBSCRIBED",
        b"UNSUBSCRIBED",
        b"DELIVERED",
        b"TTL_VALUE",
        b"MESSAGE",
    )
}


def _enc_text(value: str, what: str, max_chars: int) -> bytes:
    if not value:
        raise ProtocolError(f"{what} must not be empty")
    if len(value) > max_chars:
        raise ProtocolError(f"{what} exceeds {max_chars} characters")
    raw = _encode_utf8(value)
    return pack_u32(len(raw)) + raw


def _enc_key(value: str) -> bytes:
    return _enc_text(value, "key", MAX_KEY_CHARS)


def _enc_channel(value: str) -> bytes:
    return _enc_text(value, "channel", MAX_CHANNEL_CHARS)


def _enc_bytes(value: bytes) -> bytes:
    if len(value) > MAX_FIELD_BYTES:
        raise ProtocolError(f"field too long: {len(value)}")
    return pack_u32(len(value)) + value


def _enc_free_text(value: str) -> bytes:
    return _enc_bytes(_encode_utf8(value))


def _enc_u32(value: int) -> bytes:
    if not 0 <= value <= 0xFFFFFFFF:
        raise ProtocolError(f"u32 integer out of range: {value}")
    return pack_u32(value)


def _enc_u64(value: int) -> bytes:
    if not 0 <= value <= 0xFFFFFFFFFFFFFFFF:
        raise ProtocolError(f"u64 integer out of range: {value}")
    return _U64.pack(value)


def _encode_get(value: Get) -> bytes:
    return _PREFIXES[b"GET"] + _enc_key(value.key)


def _encode_set(value: Set) -> bytes:
    return _PREFIXES[b"SET"] + _enc_key(value.key) + _enc_bytes(value.value)


def _encode_expire(value: Expire) -> bytes:
    return _PREFIXES[b"EXPIRE"] + _enc_key(value.key) + _enc_u64(value.ms)


def _encode_delete(value: Delete) -> bytes:
    return _PREFIXES[b"DEL"] + _enc_key(value.key)


def _encode_ttl(value: TTL) -> bytes:
    return _PREFIXES[b"TTL"] + _enc_key(value.key)


def _encode_persist(value: Persist) -> bytes:
    return _PREFIXES[b"PERSIST"] + _enc_key(value.key)


def _encode_subscribe(value: Subscribe) -> bytes:
    return _PREFIXES[b"SUBSCRIBE"] + _enc_channel(value.channel)


def _encode_unsubscribe(value: Unsubscribe) -> bytes:
    return _PREFIXES[b"UNSUBSCRIBE"] + _enc_channel(value.channel)


def _encode_publish(value: Publish) -> bytes:
    return (
        _PREFIXES[b"PUBLISH"] + _enc_channel(value.channel) + _enc_bytes(value.payload)
    )


def _encode_value(value: Value) -> bytes:
    return _PREFIXES[b"VALUE"] + _enc_bytes(value.value)


def _encode_err(value: Err) -> bytes:
    return _PREFIXES[b"ERR"] + _enc_free_text(value.message)


def _encode_subscribed(value: Subscribed) -> bytes:
    return _PREFIXES[b"SUBSCRIBED"] + _enc_free_text(value.channel)


def _encode_unsubscribed(value: Unsubscribed) -> bytes:
    return _PREFIXES[b"UNSUBSCRIBED"] + _enc_free_text(value.channel)


def _encode_delivered(value: Delivered) -> bytes:
    return _PREFIXES[b"DELIVERED"] + _enc_u32(value.count)


def _encode_ttl_value(value: TTLValue) -> bytes:
    return _PREFIXES[b"TTL_VALUE"] + _enc_u64(value.ms)


def _encode_message(value: Message) -> bytes:
    return (
        _PREFIXES[b"MESSAGE"]
        + _enc_free_text(value.channel)
        + _enc_bytes(value.payload)
    )


class CommandCodec:
    """Encode and decode the fixed format, rejecting unregistered variant types."""

    def __init__(self) -> None:
        self._decoders: dict[bytes, _Decoder[Command]] = {
            b"GET": _one(Get, _read_key),
            b"SET": _pair(Set, _read_key, _bytes),
            b"EXPIRE": _pair(Expire, _read_key, _read_u64),
            b"DEL": _one(Delete, _read_key),
            b"TTL": _one(TTL, _read_key),
            b"PERSIST": _one(Persist, _read_key),
            b"SUBSCRIBE": _one(Subscribe, _read_channel),
            b"UNSUBSCRIBE": _one(Unsubscribe, _read_channel),
            b"PUBLISH": _pair(Publish, _read_channel, _bytes),
        }

    def decode(self, data: bytes) -> Command:
        return _dispatch(self._decoders, data, "command")

    def encode(self, value: Command) -> bytes:
        if type(value) is Get:
            return _encode_get(value)
        if type(value) is Set:
            return _encode_set(value)
        if type(value) is Expire:
            return _encode_expire(value)
        if type(value) is Delete:
            return _encode_delete(value)
        if type(value) is TTL:
            return _encode_ttl(value)
        if type(value) is Persist:
            return _encode_persist(value)
        if type(value) is Subscribe:
            return _encode_subscribe(value)
        if type(value) is Unsubscribe:
            return _encode_unsubscribe(value)
        if type(value) is Publish:
            return _encode_publish(value)
        raise ProtocolError(f"unsupported command: {type(value).__name__}")


class ServerMessageCodec:
    """Encode and decode replies and pushes with unrestricted reply-channel text.

    Reply and push channels may be empty or exceed request-channel character
    limits. All strings and payloads still obey the encoded field-byte limit.
    """

    def __init__(self) -> None:
        self._decoders: dict[bytes, _Decoder[ServerFrame]] = {
            b"OK": _empty(OK),
            b"NIL": _empty(NIL),
            b"PERSISTENT": _empty(PERSISTENT),
            b"VALUE": _one(Value, _bytes),
            b"ERR": _one(Err, _free_text),
            b"SUBSCRIBED": _one(Subscribed, _free_text),
            b"UNSUBSCRIBED": _one(Unsubscribed, _free_text),
            b"DELIVERED": _one(Delivered, _read_u32),
            b"TTL_VALUE": _one(TTLValue, _read_u64),
            b"MESSAGE": _pair(Message, _free_text, _bytes),
        }

    def decode(self, data: bytes) -> ServerFrame:
        return _dispatch(self._decoders, data, "server message")

    def encode(self, value: ServerFrame) -> bytes:
        if type(value) is Value:
            return _encode_value(value)
        if type(value) is Ok:
            return _PREFIXES[b"OK"]
        if type(value) is Nil:
            return _PREFIXES[b"NIL"]
        if type(value) is Persistent:
            return _PREFIXES[b"PERSISTENT"]
        if type(value) is Err:
            return _encode_err(value)
        if type(value) is Subscribed:
            return _encode_subscribed(value)
        if type(value) is Unsubscribed:
            return _encode_unsubscribed(value)
        if type(value) is Delivered:
            return _encode_delivered(value)
        if type(value) is TTLValue:
            return _encode_ttl_value(value)
        if type(value) is Message:
            return _encode_message(value)
        raise ProtocolError(f"unsupported server message: {type(value).__name__}")
