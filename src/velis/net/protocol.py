"""Composable command/frame codecs and bounded TCP message framing.

Each message is a u32 body length, a u64 request ID and the encoded body.
ID zero is reserved for server pushes and connection-wide errors.
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
    Ttl,
    TtlValue,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.errors import ProtocolError

MAX_LEN = 1 << 20
MAX_FRAME_LEN = 2 * MAX_LEN
MAX_REQUEST_ID = (1 << 64) - 1

type Frame = Reply | Message


@dataclass(frozen=True, slots=True)
class Request:
    """A connection-local, nonzero request ID and its command."""

    request_id: int
    command: Command


@dataclass(frozen=True, slots=True)
class Response:
    """A reply echoing its request ID; zero denotes a connection-wide Err."""

    request_id: int
    reply: Reply


type WireFrame = Response | Message

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


def _text_field(what: str, max_chars: int) -> CodecChain[str, bytes]:
    # A Unicode character occupies at most four bytes in UTF-8.
    return (
        CodecChain.of(Validate(what, _text_rule(what, max_chars)))
        | Utf8()
        | LengthPrefix(max_chars * 4)
    )


KEY = _text_field("key", 512)
CHANNEL = _text_field("channel", 256)


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
            case Delete(key):
                return _tagged("DEL", KEY.encode(key))
            case Ttl(key):
                return _tagged("TTL", KEY.encode(key))
            case Persist(key):
                return _tagged("PERSIST", KEY.encode(key))
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
            case "DEL":
                command = Delete(cursor.read(KEY))
            case "TTL":
                command = Ttl(cursor.read(KEY))
            case "PERSIST":
                command = Persist(cursor.read(KEY))
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
            case TtlValue(ms):
                return _tagged("TTL_VALUE", U64.encode(ms))
            case Persistent():
                return _tagged("PERSISTENT")
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
            case "TTL_VALUE":
                frame = TtlValue(cursor.read_uint(U64))
            case "PERSISTENT":
                frame = Persistent()
            case "MESSAGE":
                frame = Message(cursor.read(TEXT), cursor.read(BYTES))
            case _:
                raise ProtocolError(f"unknown frame: {tag[:40]!r}")
        cursor.finish()
        return frame


@dataclass(frozen=True, slots=True)
class _RequestCodec:
    body: Codec[Command, bytes]

    @property
    def name(self) -> str:
        return f"request_{self.body.name}"

    def encode(self, request: Request) -> bytes:
        if request.request_id == 0:
            raise ProtocolError("request ID must be nonzero")
        return U64.encode(request.request_id) + self.body.encode(request.command)

    def decode(self, data: bytes) -> Request:
        request_id = U64.decode(data[: U64.width])
        if request_id == 0:
            raise ProtocolError("request ID must be nonzero")
        return Request(request_id, self.body.decode(data[U64.width :]))


@dataclass(frozen=True, slots=True)
class _ResponseCodec:
    body: Codec[Frame, bytes]

    @property
    def name(self) -> str:
        return f"response_{self.body.name}"

    def encode(self, frame: WireFrame) -> bytes:
        if isinstance(frame, Message):
            return U64.encode(0) + self.body.encode(frame)
        if frame.request_id == 0 and not isinstance(frame.reply, Err):
            raise ProtocolError("only connection errors may use reply ID zero")
        return U64.encode(frame.request_id) + self.body.encode(frame.reply)

    def decode(self, data: bytes) -> WireFrame:
        request_id = U64.decode(data[: U64.width])
        frame = self.body.decode(data[U64.width :])
        if isinstance(frame, Message):
            if request_id != 0:
                raise ProtocolError("server pushes must use request ID zero")
            return frame
        if request_id == 0 and not isinstance(frame, Err):
            raise ProtocolError("only connection errors may use reply ID zero")
        return Response(request_id, frame)


@dataclass(frozen=True)
class WireProtocol:
    """Use matching body codecs on both peers; framing is always outermost.

    Request IDs wrap the body codecs and precede their encoded output, including
    any compression. Lengths are bounded before reading bodies. Failures on
    malformed input should raise ProtocolError. Codecs must be stateless or
    safe to share across connections.
    """

    command_codec: Codec[Command, bytes] = CommandCodec()
    frame_codec: Codec[Frame, bytes] = FrameCodec()
    framing: LengthPrefix = field(default_factory=lambda: LengthPrefix(MAX_FRAME_LEN))

    @cached_property
    def _requests(self) -> _RequestCodec:
        return _RequestCodec(self.command_codec)

    @cached_property
    def _responses(self) -> _ResponseCodec:
        return _ResponseCodec(self.frame_codec)

    @cached_property
    def commands(self) -> CodecChain[Request, bytes]:
        return CodecChain.of(self._requests) | self.framing

    @cached_property
    def frames(self) -> CodecChain[WireFrame, bytes]:
        return CodecChain.of(self._responses) | self.framing

    async def _read(self, reader: asyncio.StreamReader) -> bytes:
        header = await reader.readexactly(U32.width)
        length = self.framing.decode_length(header)
        return await reader.readexactly(length)

    async def read_command(self, reader: asyncio.StreamReader) -> Request:
        return self._requests.decode(await self._read(reader))

    async def read_frame(self, reader: asyncio.StreamReader) -> WireFrame:
        return self._responses.decode(await self._read(reader))


DEFAULT_PROTOCOL = WireProtocol()
