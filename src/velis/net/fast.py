"""Offset-based codecs for Velis's fixed command and server-message bodies.

Decoders require bytes, reject trailing data and check field limits before
reading or decoding their bodies. Tags and field order are fixed by the protocol.
"""

from collections.abc import Callable
from struct import Struct
from typing import Any

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
_unpack64, _pack64 = _U64.unpack_from, _U64.pack
_BAD_UTF8 = "invalid utf-8 in string field"
_TRAILING = "trailing bytes after record"

type ServerFrame = Reply | Message
type _Decoder[T] = Callable[[bytes, int, int], T]
type _Encoder = Callable[[Any], bytes]


def _truncated(need: int) -> ProtocolError:
    return ProtocolError(f"truncated record: need {need} bytes")


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
    try:
        text = data[start:stop].decode()
    except UnicodeError as exc:
        raise ProtocolError(_BAD_UTF8) from exc
    if not text:
        raise ProtocolError(f"{what} must not be empty")
    if len(text) > max_chars:
        raise ProtocolError(f"{what} exceeds {max_chars} characters")
    return text, stop


def _free_text(data: bytes, pos: int, end: int) -> tuple[str, int]:
    start, stop = _span(data, pos, end, MAX_FIELD_BYTES)
    try:
        return data[start:stop].decode(), stop
    except UnicodeError as exc:
        raise ProtocolError(_BAD_UTF8) from exc


def _text_only[T](cls: Callable[[str], T], what: str, max_chars: int) -> _Decoder[T]:
    def decode(data: bytes, pos: int, end: int) -> T:
        text, pos = _text(data, pos, end, what, max_chars)
        if pos != end:
            raise ProtocolError(_TRAILING)
        return cls(text)

    return decode


def _text_bytes[T](
    cls: Callable[[str, bytes], T], what: str, max_chars: int
) -> _Decoder[T]:
    def decode(data: bytes, pos: int, end: int) -> T:
        text, pos = _text(data, pos, end, what, max_chars)
        start, stop = _span(data, pos, end, MAX_FIELD_BYTES)
        if stop != end:
            raise ProtocolError(_TRAILING)
        return cls(text, data[start:stop])

    return decode


def _text_u64[T](
    cls: Callable[[str, int], T], what: str, max_chars: int
) -> _Decoder[T]:
    def decode(data: bytes, pos: int, end: int) -> T:
        text, pos = _text(data, pos, end, what, max_chars)
        if pos + 8 > end:
            raise _truncated(8)
        (number,) = _unpack64(data, pos)
        if pos + 8 != end:
            raise ProtocolError(_TRAILING)
        return cls(text, number)

    return decode


def _empty[T](value: T) -> _Decoder[T]:
    def decode(data: bytes, pos: int, end: int) -> T:
        if pos != end:
            raise ProtocolError(_TRAILING)
        return value

    return decode


def _free_text_only[T](cls: Callable[[str], T]) -> _Decoder[T]:
    def decode(data: bytes, pos: int, end: int) -> T:
        text, pos = _free_text(data, pos, end)
        if pos != end:
            raise ProtocolError(_TRAILING)
        return cls(text)

    return decode


def _bytes_only[T](cls: Callable[[bytes], T]) -> _Decoder[T]:
    def decode(data: bytes, pos: int, end: int) -> T:
        start, stop = _span(data, pos, end, MAX_FIELD_BYTES)
        if stop != end:
            raise ProtocolError(_TRAILING)
        return cls(data[start:stop])

    return decode


def _number_only[T](cls: Callable[[int], T], integer: Struct) -> _Decoder[T]:
    size = integer.size
    unpack = integer.unpack_from

    def decode(data: bytes, pos: int, end: int) -> T:
        if pos + size > end:
            raise _truncated(size)
        (number,) = unpack(data, pos)
        if pos + size != end:
            raise ProtocolError(_TRAILING)
        return cls(number)

    return decode


def _message(data: bytes, pos: int, end: int) -> Message:
    channel, pos = _free_text(data, pos, end)
    start, stop = _span(data, pos, end, MAX_FIELD_BYTES)
    if stop != end:
        raise ProtocolError(_TRAILING)
    return Message(channel, data[start:stop])


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
    try:
        raw = value.encode()
    except UnicodeError as exc:
        raise ProtocolError(_BAD_UTF8) from exc
    return pack_u32(len(raw)) + raw


def _enc_free_text(value: str) -> bytes:
    try:
        raw = value.encode()
    except UnicodeError as exc:
        raise ProtocolError(_BAD_UTF8) from exc
    return _enc_bytes(raw)


def _enc_bytes(value: bytes) -> bytes:
    if len(value) > MAX_FIELD_BYTES:
        raise ProtocolError(f"field too long: {len(value)}")
    return pack_u32(len(value)) + value


def _enc_u32(value: int) -> bytes:
    if not 0 <= value <= 0xFFFFFFFF:
        raise ProtocolError(f"u32 integer out of range: {value}")
    return pack_u32(value)


def _enc_u64(value: int) -> bytes:
    if not 0 <= value <= 0xFFFFFFFFFFFFFFFF:
        raise ProtocolError(f"u64 integer out of range: {value}")
    return _pack64(value)


class CommandCodec:
    """Encode and decode every command in the fixed client-to-server format."""

    def __init__(self) -> None:
        key, channel = MAX_KEY_CHARS, MAX_CHANNEL_CHARS
        self._decoders: dict[bytes, _Decoder[Command]] = {
            b"GET": _text_only(Get, "key", key),
            b"SET": _text_bytes(Set, "key", key),
            b"EXPIRE": _text_u64(Expire, "key", key),
            b"DEL": _text_only(Delete, "key", key),
            b"TTL": _text_only(TTL, "key", key),
            b"PERSIST": _text_only(Persist, "key", key),
            b"SUBSCRIBE": _text_only(Subscribe, "channel", channel),
            b"UNSUBSCRIBE": _text_only(Unsubscribe, "channel", channel),
            b"PUBLISH": _text_bytes(Publish, "channel", channel),
        }
        self._encoders: dict[type[Any], _Encoder] = {
            Get: lambda c, p=_PREFIXES[b"GET"]: p + _enc_text(c.key, "key", key),
            Set: lambda c, p=_PREFIXES[b"SET"]: (
                p + _enc_text(c.key, "key", key) + _enc_bytes(c.value)
            ),
            Expire: lambda c, p=_PREFIXES[b"EXPIRE"]: (
                p + _enc_text(c.key, "key", key) + _enc_u64(c.ms)
            ),
            Delete: lambda c, p=_PREFIXES[b"DEL"]: p + _enc_text(c.key, "key", key),
            TTL: lambda c, p=_PREFIXES[b"TTL"]: p + _enc_text(c.key, "key", key),
            Persist: lambda c, p=_PREFIXES[b"PERSIST"]: (
                p + _enc_text(c.key, "key", key)
            ),
            Subscribe: lambda c, p=_PREFIXES[b"SUBSCRIBE"]: (
                p + _enc_text(c.channel, "channel", channel)
            ),
            Unsubscribe: lambda c, p=_PREFIXES[b"UNSUBSCRIBE"]: (
                p + _enc_text(c.channel, "channel", channel)
            ),
            Publish: lambda c, p=_PREFIXES[b"PUBLISH"]: (
                p + _enc_text(c.channel, "channel", channel) + _enc_bytes(c.payload)
            ),
        }

    def decode(self, data: bytes) -> Command:
        end = len(data)
        start, stop = _span(data, 0, end, MAX_TAG_BYTES)
        tag = data[start:stop]
        decoder = self._decoders.get(tag)
        if decoder is None:
            try:
                name = tag.decode()
            except UnicodeError as exc:
                raise ProtocolError(_BAD_UTF8) from exc
            raise ProtocolError(f"unknown command: {name!r}")
        return decoder(data, stop, end)

    def encode(self, value: Command) -> bytes:
        encoder = self._encoders.get(type(value))
        if encoder is None:
            raise ProtocolError(f"unsupported command: {type(value).__name__}")
        return encoder(value)


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
            b"VALUE": _bytes_only(Value),
            b"ERR": _free_text_only(Err),
            b"SUBSCRIBED": _free_text_only(Subscribed),
            b"UNSUBSCRIBED": _free_text_only(Unsubscribed),
            b"DELIVERED": _number_only(Delivered, _U32),
            b"TTL_VALUE": _number_only(TTLValue, _U64),
            b"MESSAGE": _message,
        }
        ok, nil, persistent = (
            _PREFIXES[b"OK"],
            _PREFIXES[b"NIL"],
            _PREFIXES[b"PERSISTENT"],
        )
        self._encoders: dict[type[Any], _Encoder] = {
            Ok: lambda m: ok,
            Nil: lambda m: nil,
            Persistent: lambda m: persistent,
            Value: lambda m, p=_PREFIXES[b"VALUE"]: p + _enc_bytes(m.value),
            Err: lambda m, p=_PREFIXES[b"ERR"]: p + _enc_free_text(m.message),
            Subscribed: lambda m, p=_PREFIXES[b"SUBSCRIBED"]: (
                p + _enc_free_text(m.channel)
            ),
            Unsubscribed: lambda m, p=_PREFIXES[b"UNSUBSCRIBED"]: (
                p + _enc_free_text(m.channel)
            ),
            Delivered: lambda m, p=_PREFIXES[b"DELIVERED"]: p + _enc_u32(m.count),
            TTLValue: lambda m, p=_PREFIXES[b"TTL_VALUE"]: p + _enc_u64(m.ms),
            Message: lambda m, p=_PREFIXES[b"MESSAGE"]: (
                p + _enc_free_text(m.channel) + _enc_bytes(m.payload)
            ),
        }

    def decode(self, data: bytes) -> ServerFrame:
        end = len(data)
        start, stop = _span(data, 0, end, MAX_TAG_BYTES)
        tag = data[start:stop]
        decoder = self._decoders.get(tag)
        if decoder is None:
            try:
                name = tag.decode()
            except UnicodeError as exc:
                raise ProtocolError(_BAD_UTF8) from exc
            raise ProtocolError(f"unknown server message: {name!r}")
        return decoder(data, stop, end)

    def encode(self, value: ServerFrame) -> bytes:
        encoder = self._encoders.get(type(value))
        if encoder is None:
            raise ProtocolError(f"unsupported server message: {type(value).__name__}")
        return encoder(value)
