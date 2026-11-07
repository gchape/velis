"""Composable command/frame codecs and bounded TCP message framing.

Each message is a u32 body length followed by the encoded body. Field
schemas and validation are shared by synchronous encoding and decoding.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from velis.codec import Codec, CodecChain, LengthPrefix, Utf8
from velis.codec.binary import U32, U64
from velis.codec.records import Field, Record, TaggedCodec
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

MAX_LEN = 1 << 20
MAX_FRAME_LEN = 8 << 20

type Frame = Reply | Message

BYTES = CodecChain.of(LengthPrefix(MAX_LEN))
TEXT = CodecChain.of(Utf8()) | BYTES

COMMANDS: CodecChain[Command, bytes] = CodecChain.of(
    TaggedCodec[Command](
        "command",
        TEXT,
        (
            Record(Get, "GET", (Field("key", TEXT),)),
            Record(Set, "SET", (Field("key", TEXT), Field("value", BYTES))),
            Record(Expire, "EXPIRE", (Field("key", TEXT), Field("ms", U64, U64.width))),
            Record(Subscribe, "SUBSCRIBE", (Field("channel", TEXT),)),
            Record(Unsubscribe, "UNSUBSCRIBE", (Field("channel", TEXT),)),
            Record(
                Publish, "PUBLISH", (Field("channel", TEXT), Field("payload", BYTES))
            ),
        ),
    )
)
FRAMES: CodecChain[Frame, bytes] = CodecChain.of(
    TaggedCodec[Frame](
        "frame",
        TEXT,
        (
            Record(Ok, "OK"),
            Record(Nil, "NIL"),
            Record(Value, "VALUE", (Field("value", BYTES),)),
            Record(Err, "ERR", (Field("message", TEXT),)),
            Record(Subscribed, "SUBSCRIBED", (Field("channel", TEXT),)),
            Record(Unsubscribed, "UNSUBSCRIBED", (Field("channel", TEXT),)),
            Record(Delivered, "DELIVERED", (Field("count", U32, U32.width),)),
            Record(
                Message, "MESSAGE", (Field("channel", TEXT), Field("payload", BYTES))
            ),
        ),
    )
)


@dataclass(frozen=True, slots=True)
class WireProtocol:
    """Use matching body codecs on both peers; framing is always outermost.

    Lengths are bounded before reading bodies. Codec and rule failures on
    malformed input should raise ProtocolError. Codecs must be stateless or
    safe to share across connections.
    """

    command_codec: Codec[Command, bytes] = COMMANDS
    frame_codec: Codec[Frame, bytes] = FRAMES
    framing: LengthPrefix = field(default_factory=lambda: LengthPrefix(MAX_FRAME_LEN))
    commands: CodecChain[Command, bytes] = field(init=False)
    frames: CodecChain[Frame, bytes] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "commands", CodecChain.of(self.command_codec) | self.framing
        )
        object.__setattr__(
            self, "frames", CodecChain.of(self.frame_codec) | self.framing
        )

    async def _read(self, reader: asyncio.StreamReader) -> bytes:
        header = await reader.readexactly(U32.width)
        length = self.framing.decode_length(header)
        return header + await reader.readexactly(length)

    async def read_command(self, reader: asyncio.StreamReader) -> Command:
        return self.commands.decode(await self._read(reader))

    async def read_frame(self, reader: asyncio.StreamReader) -> Frame:
        return self.frames.decode(await self._read(reader))


DEFAULT_PROTOCOL = WireProtocol()


def encode_command(command: Command) -> bytes:
    return DEFAULT_PROTOCOL.commands.encode(command)


def encode_reply(reply: Reply) -> bytes:
    return DEFAULT_PROTOCOL.frames.encode(reply)


def encode_message(channel: str, payload: bytes) -> bytes:
    return DEFAULT_PROTOCOL.frames.encode(Message(channel, payload))


async def read_command(reader: asyncio.StreamReader) -> Command:
    return await DEFAULT_PROTOCOL.read_command(reader)


async def read_frame(reader: asyncio.StreamReader) -> Frame:
    return await DEFAULT_PROTOCOL.read_frame(reader)
