"""Velis's fixed binary wire format and bounded u32-length framing.

Commands and server messages use tagged bodies with fixed field layouts. Values
and publication payloads are opaque bytes; framing is not configurable.
Reply and push channels obey a byte limit, without the request-channel
character and emptiness restrictions.

Each direction's frame limit is derived from its codec table, so the largest
frame a peer may send is exactly the largest message the table can describe.
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
from velis.net.limits import MAX_CHANNEL_BYTES, MAX_CHANNEL_CHARS, MAX_KEY_CHARS
from velis.net.wire import (
    BLOB,
    FREE_TEXT,
    U32,
    U64,
    Codec,
    free_text,
    msg,
    pack_u32,
    text,
    unpack_u32,
)

__all__ = [
    "COMMANDS",
    "MAX_COMMAND_BODY_BYTES",
    "MAX_SERVER_BODY_BYTES",
    "SERVER_MESSAGES",
    "PushEncoder",
    "ServerFrame",
    "encode_command",
    "encode_frame",
    "encode_message",
    "encode_push",
    "iter_frame_spans",
    "iter_frames",
    "read_frame",
]

type ServerFrame = Reply | Message

KEY = text("key", MAX_KEY_CHARS)
CHANNEL = text("channel", MAX_CHANNEL_CHARS)
ECHOED_CHANNEL = free_text(MAX_CHANNEL_BYTES)

COMMANDS = Codec[Command](
    "command",
    {
        Get: msg(b"GET", key=KEY),
        Set: msg(b"SET", key=KEY, value=BLOB),
        Expire: msg(b"EXPIRE", key=KEY, ms=U64),
        Delete: msg(b"DEL", key=KEY),
        TTL: msg(b"TTL", key=KEY),
        Persist: msg(b"PERSIST", key=KEY),
        Subscribe: msg(b"SUBSCRIBE", channel=CHANNEL),
        Unsubscribe: msg(b"UNSUBSCRIBE", channel=CHANNEL),
        Publish: msg(b"PUBLISH", channel=CHANNEL, payload=BLOB),
    },
    covers=Command,
)

SERVER_MESSAGES = Codec[ServerFrame](
    "server message",
    {
        Ok: msg(b"OK"),
        Nil: msg(b"NIL"),
        Persistent: msg(b"PERSISTENT"),
        Value: msg(b"VALUE", value=BLOB),
        Err: msg(b"ERR", message=FREE_TEXT),
        Subscribed: msg(b"SUBSCRIBED", channel=ECHOED_CHANNEL),
        Unsubscribed: msg(b"UNSUBSCRIBED", channel=ECHOED_CHANNEL),
        Delivered: msg(b"DELIVERED", count=U32),
        TTLValue: msg(b"TTL_VALUE", ms=U64),
        Message: msg(b"MESSAGE", channel=ECHOED_CHANNEL, payload=BLOB),
    },
    covers=ServerFrame,
)

MAX_COMMAND_BODY_BYTES = COMMANDS.max_body
MAX_SERVER_BODY_BYTES = SERVER_MESSAGES.max_body


def _check_frame_length(length: int, limit: int) -> None:
    if length > limit:
        raise ProtocolError(f"field too long: {length}")


def encode_frame(body: bytes, limit: int) -> bytes:
    """Frame one body with its u32 byte length, rejecting bodies above limit."""
    _check_frame_length(len(body), limit)
    return pack_u32(len(body)) + body


def encode_command(command: Command) -> bytes:
    """Encode a command and its outer frame before any bytes are written."""
    return encode_frame(COMMANDS.encode(command), MAX_COMMAND_BODY_BYTES)


def encode_message(message: ServerFrame) -> bytes:
    """Encode a reply or publication push and its complete outer frame."""
    return encode_frame(SERVER_MESSAGES.encode(message), MAX_SERVER_BODY_BYTES)


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


def iter_frame_spans(
    buf: bytearray, limit: int = MAX_COMMAND_BODY_BYTES
) -> Iterator[tuple[int, int]]:
    """Yield each complete body's start and end offsets without copying it.

    The default limit suits the server reading commands. Partial frames remain
    buffered. Oversized headers fail before their bodies arrive. Do not mutate
    buf until iteration ends; discard only the consumed prefix afterward.
    Offsets can be passed directly to Codec.decode; no buffer views are retained.
    """
    pos = 0
    while len(buf) - pos >= 4:
        (length,) = unpack_u32(buf, pos)
        _check_frame_length(length, limit)
        end = pos + 4 + length
        if len(buf) < end:
            return
        yield pos + 4, end
        pos = end


def iter_frames(
    buf: bytearray, limit: int = MAX_COMMAND_BODY_BYTES
) -> Iterator[tuple[bytes, int]]:
    """Yield owned body bytes and end offsets; leave buf untouched."""
    for start, end in iter_frame_spans(buf, limit):
        # Release the view before yielding so callers can safely resize later.
        with memoryview(buf) as view:
            body = bytes(view[start:end])
        yield body, end


async def read_frame(
    reader: asyncio.StreamReader, limit: int = MAX_SERVER_BODY_BYTES
) -> bytes | None:
    """Read a body, returning None only for clean EOF at a frame boundary.

    The default limit suits the client reading server messages. Partial headers
    and bodies raise IncompleteReadError. Check the body limit immediately
    after reading the header, before waiting for the body bytes.
    """
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError as exc:
        if exc.partial:
            raise
        return None
    (length,) = unpack_u32(header)
    _check_frame_length(length, limit)
    return await reader.readexactly(length)
