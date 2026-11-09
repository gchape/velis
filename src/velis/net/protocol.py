"""Velis's fixed binary wire format and bounded u32-length framing.

Commands and server messages use tagged bodies with fixed field layouts. Values
and publication payloads are opaque bytes; framing is not configurable.
Reply and push channels obey the field-byte limit without request-channel
character restrictions.
"""

import asyncio
from collections.abc import Callable, Iterator

from velis.core.messages import (
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
from velis.net.limits import MAX_CHANNEL_CHARS, MAX_FRAME_BODY_BYTES, MAX_KEY_CHARS
from velis.net.wire import BLOB, FREE_TEXT, U32, U64, Codec, pack_u32, text, unpack_u32

__all__ = [
    "COMMANDS",
    "SERVER_MESSAGES",
    "PushEncoder",
    "ServerFrame",
    "encode_command",
    "encode_frame",
    "encode_message",
    "encode_push",
    "iter_frames",
    "read_frame",
]

type ServerFrame = Reply | Message

KEY = text("key", MAX_KEY_CHARS)
CHANNEL = text("channel", MAX_CHANNEL_CHARS)

COMMANDS = Codec[Command](
    "command",
    {
        Get: (b"GET", KEY),
        Set: (b"SET", KEY, BLOB),
        Expire: (b"EXPIRE", KEY, U64),
        Delete: (b"DEL", KEY),
        TTL: (b"TTL", KEY),
        Persist: (b"PERSIST", KEY),
        Subscribe: (b"SUBSCRIBE", CHANNEL),
        Unsubscribe: (b"UNSUBSCRIBE", CHANNEL),
        Publish: (b"PUBLISH", CHANNEL, BLOB),
    },
)

SERVER_MESSAGES = Codec[ServerFrame](
    "server message",
    {
        Ok: (b"OK",),
        Nil: (b"NIL",),
        Persistent: (b"PERSISTENT",),
        Value: (b"VALUE", BLOB),
        Err: (b"ERR", FREE_TEXT),
        Subscribed: (b"SUBSCRIBED", FREE_TEXT),
        Unsubscribed: (b"UNSUBSCRIBED", FREE_TEXT),
        Delivered: (b"DELIVERED", U32),
        TTLValue: (b"TTL_VALUE", U64),
        Message: (b"MESSAGE", FREE_TEXT, BLOB),
    },
)


def _check_frame_length(length: int) -> None:
    if length > MAX_FRAME_BODY_BYTES:
        raise ProtocolError(f"field too long: {length}")


def encode_frame(body: bytes) -> bytes:
    """Frame one body with its u32 byte length, rejecting oversized bodies."""
    _check_frame_length(len(body))
    return pack_u32(len(body)) + body


def encode_command(command: Command) -> bytes:
    """Encode a command and its outer frame before any bytes are written."""
    return encode_frame(COMMANDS.encode(command))


def encode_message(message: ServerFrame) -> bytes:
    """Encode a reply or publication push and its complete outer frame."""
    return encode_frame(SERVER_MESSAGES.encode(message))


class PushEncoder:
    """Cache one framed push for consecutive deliveries of the same object.

    Equal but distinct pushes encode separately. An encoding failure preserves
    the previous cached result. Nested publications can replace the cache and
    cause the outer push to encode again without changing its bytes.
    """

    def __init__(self, encode: Callable[[Message], bytes]) -> None:
        self._encode = encode
        self._last: tuple[Message, bytes] | None = None

    def __call__(self, message: Message) -> bytes:
        last = self._last
        if last is None or last[0] is not message:
            self._last = last = (message, self._encode(message))
        return last[1]


encode_push = PushEncoder(encode_message)


def iter_frames(buf: bytearray) -> Iterator[tuple[bytes, int]]:
    """Yield each complete frame's body and end offset without consuming buf.

    Partial frames remain buffered. Oversized headers fail before their bodies
    arrive. Do not mutate buf until iteration ends; discard only the consumed
    prefix afterward. Bodies are bytes, suitable for the offset-based decoders.
    """
    pos = 0
    while len(buf) - pos >= 4:
        (length,) = unpack_u32(buf, pos)
        _check_frame_length(length)
        end = pos + 4 + length
        if len(buf) < end:
            return
        yield bytes(buf[pos + 4 : end]), end
        pos = end


async def read_frame(reader: asyncio.StreamReader) -> bytes | None:
    """Read a body, returning None only for clean EOF at a frame boundary.

    Partial headers and bodies raise IncompleteReadError. Check the body limit
    immediately after reading the header, before waiting for the body bytes.
    """
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError as exc:
        if exc.partial:
            raise
        return None
    (length,) = unpack_u32(header)
    _check_frame_length(length)
    return await reader.readexactly(length)
