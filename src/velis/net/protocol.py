"""Composable command/frame codecs and bounded TCP message framing.

Each message is a u32 body length followed by the encoded body. A body is
a length-prefixed text tag followed by that tag's fields.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import cached_property
from typing import assert_never

from velis.codec import Codec, CodecChain, LengthPrefix, Utf8, Validate
from velis.codec.binary import U32, U64
from velis.codec.cursor import Cursor
from velis.core.commands import (
    Command,
    Delivered,
    Err,
    Expire,
    Get,
    Message,
    Nil,
    Ok,
    Publish,
    Reply,
    Set,
    Subscribe,
    Subscribed,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.errors import ProtocolError

MAX_LEN = 1 << 20
MAX_FRAME_LEN = 2 * MAX_LEN

type Frame = Reply | Message

BYTES = LengthPrefix(MAX_LEN)
TEXT = CodecChain.of(Utf8()) | BYTES
TAG = CodecChain.of(Utf8()) | LengthPrefix(32)


def _text_rule(what: str, max_chars: int) -> Callable[[str], None]:
    def check(value: str) -> None:
        if not value:
            raise ProtocolError(f"{what} must not be empty")
        if len(value) > max_chars:
            raise ProtocolError(f"{what} exceeds {max_chars} characters")

    return check


KEY = (
    CodecChain.of(Validate("key", _text_rule("key", 512)))
    | Utf8()
    | LengthPrefix(512 * 4)
)
CHANNEL = (
    CodecChain.of(Validate("channel", _text_rule("channel", 256)))
    | Utf8()
    | LengthPrefix(256 * 4)
)


def _tagged(tag: str, *fields: bytes) -> bytes:
    return TAG.encode(tag) + b"".join(fields)


@dataclass(frozen=True, slots=True)
class CommandCodec:
    name: str = "command"

    def encode(self, command: Command) -> bytes:
        match command:
            case Get(key):
                return _tagged("GET", KEY.encode(key))
            case Set(key, value):
                return _tagged("SET", KEY.encode(key), BYTES.encode(value))
            case Expire(key, ms):
                return _tagged("EXPIRE", KEY.encode(key), U64.encode(ms))
            case Subscribe(channel):
                return _tagged("SUBSCRIBE", CHANNEL.encode(channel))
            case Unsubscribe(channel):
                return _tagged("UNSUBSCRIBE", CHANNEL.encode(channel))
            case Publish(channel, payload):
                return _tagged(
                    "PUBLISH", CHANNEL.encode(channel), BYTES.encode(payload)
                )
            case _:
                assert_never(command)

    def decode(self, data: bytes) -> Command:
        cursor = Cursor(data)
        tag = cursor.read(TAG)
        command: Command
        match tag:
            case "GET":
                command = Get(cursor.read(KEY))
            case "SET":
                command = Set(cursor.read(KEY), cursor.read(BYTES))
            case "EXPIRE":
                command = Expire(cursor.read(KEY), cursor.read_uint(U64))
            case "SUBSCRIBE":
                command = Subscribe(cursor.read(CHANNEL))
            case "UNSUBSCRIBE":
                command = Unsubscribe(cursor.read(CHANNEL))
            case "PUBLISH":
                command = Publish(cursor.read(CHANNEL), cursor.read(BYTES))
            case _:
                raise ProtocolError(f"unknown command: {tag[:40]!r}")
        cursor.finish()
        return command


@dataclass(frozen=True, slots=True)
class FrameCodec:
    name: str = "frame"

    def encode(self, frame: Frame) -> bytes:
        match frame:
            case Ok():
                return _tagged("OK")
            case Nil():
                return _tagged("NIL")
            case Value(value):
                return _tagged("VALUE", BYTES.encode(value))
            case Err(message):
                return _tagged("ERR", TEXT.encode(message))
            case Subscribed(channel):
                return _tagged("SUBSCRIBED", TEXT.encode(channel))
            case Unsubscribed(channel):
                return _tagged("UNSUBSCRIBED", TEXT.encode(channel))
            case Delivered(count):
                return _tagged("DELIVERED", U32.encode(count))
            case Message(channel, payload):
                return _tagged("MESSAGE", TEXT.encode(channel), BYTES.encode(payload))
            case _:
                assert_never(frame)

    def decode(self, data: bytes) -> Frame:
        cursor = Cursor(data)
        tag = cursor.read(TAG)
        frame: Frame
        match tag:
            case "OK":
                frame = Ok()
            case "NIL":
                frame = Nil()
            case "VALUE":
                frame = Value(cursor.read(BYTES))
            case "ERR":
                frame = Err(cursor.read(TEXT))
            case "SUBSCRIBED":
                frame = Subscribed(cursor.read(TEXT))
            case "UNSUBSCRIBED":
                frame = Unsubscribed(cursor.read(TEXT))
            case "DELIVERED":
                frame = Delivered(cursor.read_uint(U32))
            case "MESSAGE":
                frame = Message(cursor.read(TEXT), cursor.read(BYTES))
            case _:
                raise ProtocolError(f"unknown frame: {tag[:40]!r}")
        cursor.finish()
        return frame


@dataclass(frozen=True)
class WireProtocol:
    """Use matching body codecs on both peers; framing is always outermost.

    Lengths are bounded before reading bodies. Codec and rule failures on
    malformed input should raise ProtocolError. Codecs must be stateless or
    safe to share across connections.
    """

    command_codec: Codec[Command, bytes] = CommandCodec()
    frame_codec: Codec[Frame, bytes] = FrameCodec()
    framing: LengthPrefix = field(default_factory=lambda: LengthPrefix(MAX_FRAME_LEN))

    @cached_property
    def commands(self) -> CodecChain[Command, bytes]:
        return CodecChain.of(self.command_codec) | self.framing

    @cached_property
    def frames(self) -> CodecChain[Frame, bytes]:
        return CodecChain.of(self.frame_codec) | self.framing

    async def _read(self, reader: asyncio.StreamReader) -> bytes:
        header = await reader.readexactly(U32.width)
        length = self.framing.decode_length(header)
        return await reader.readexactly(length)

    async def read_command(self, reader: asyncio.StreamReader) -> Command:
        return self.command_codec.decode(await self._read(reader))

    async def read_frame(self, reader: asyncio.StreamReader) -> Frame:
        return self.frame_codec.decode(await self._read(reader))


DEFAULT_PROTOCOL = WireProtocol()
