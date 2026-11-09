"""Velis's fixed binary wire format and bounded u32-length framing.

Commands and server messages use tagged bodies with fixed field layouts. Values
and publication payloads are opaque bytes; framing is not configurable.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable, Iterator

from velis.core.messages import Command, Message
from velis.errors import ProtocolError
from velis.net.fast import (
    CommandCodec,
    ServerFrame,
    ServerMessageCodec,
    pack_u32,
    unpack_u32,
)
from velis.net.limits import MAX_FIELD_BYTES, MAX_FRAME_BODY_BYTES

__all__ = [
    "COMMANDS",
    "MAX_FIELD_BYTES",
    "MAX_FRAME_BODY_BYTES",
    "SERVER_MESSAGES",
    "PushEncoder",
    "ServerFrame",
    "encode_command",
    "encode_frame",
    "encode_message",
    "encode_push",
    "iter_frames",
    "read_messages",
]

COMMANDS = CommandCodec()
SERVER_MESSAGES = ServerMessageCodec()


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


async def _read_body(reader: asyncio.StreamReader) -> bytes | None:
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


async def read_messages(reader: asyncio.StreamReader) -> AsyncGenerator[ServerFrame]:
    """Yield fixed-format replies and pushes until a clean server disconnect."""
    while (body := await _read_body(reader)) is not None:
        yield SERVER_MESSAGES.decode(body)
