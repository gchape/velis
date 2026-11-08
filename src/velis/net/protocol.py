"""Composable command/frame codecs and bounded TCP message framing.

Each message is a u32 body length followed by the encoded body. Default bodies
use a text tag and ordered fields; tagged tables map each tag to its dataclass
variant and explicit field layouts. The server answers commands in order, so
replies carry no request identifiers.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass, field
from functools import cached_property

from velis.codec import (
    Codec,
    CodecChain,
    Fixed,
    FrameDecoder,
    Identity,
    LengthPrefix,
    Prefixed,
    TaggedUnion,
    Utf8,
    Validate,
)
from velis.codec.binary import U32, U64
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
READ_SIZE = 64 * 1024

type Frame = Reply | Message

BYTES = Prefixed(Identity(), MAX_LEN)
TEXT = Prefixed(Utf8(), MAX_LEN)


def _text_rule(what: str, max_chars: int) -> Callable[[str], None]:
    def check(value: str) -> None:
        if not value:
            raise ProtocolError(f"{what} must not be empty")
        if len(value) > max_chars:
            raise ProtocolError(f"{what} exceeds {max_chars} characters")

    return check


def _text_field(what: str, max_chars: int) -> Prefixed[str]:
    """Bound text by characters and allow up to four UTF-8 bytes per character."""
    return Prefixed(
        CodecChain.of(Validate(_text_rule(what, max_chars))) | Utf8(),
        max_chars * 4,
    )


KEY = _text_field("key", 512)
CHANNEL = _text_field("channel", 256)

COMMANDS: TaggedUnion[Command] = TaggedUnion(
    "command",
    {
        "GET": (Get, (KEY,)),
        "SET": (Set, (KEY, BYTES)),
        "EXPIRE": (Expire, (KEY, Fixed(U64, 8))),
        "DEL": (Delete, (KEY,)),
        "TTL": (Ttl, (KEY,)),
        "PERSIST": (Persist, (KEY,)),
        "SUBSCRIBE": (Subscribe, (CHANNEL,)),
        "UNSUBSCRIBE": (Unsubscribe, (CHANNEL,)),
        "PUBLISH": (Publish, (CHANNEL, BYTES)),
    },
)

FRAMES: TaggedUnion[Frame] = TaggedUnion(
    "frame",
    {
        "OK": (Ok, ()),
        "NIL": (Nil, ()),
        "VALUE": (Value, (BYTES,)),
        "ERR": (Err, (TEXT,)),
        "SUBSCRIBED": (Subscribed, (TEXT,)),
        "UNSUBSCRIBED": (Unsubscribed, (TEXT,)),
        "DELIVERED": (Delivered, (Fixed(U32, 4),)),
        "TTL_VALUE": (TtlValue, (Fixed(U64, 8),)),
        "PERSISTENT": (Persistent, ()),
        "MESSAGE": (Message, (TEXT, BYTES)),
    },
)


@dataclass(frozen=True)
class WireProtocol:
    """Use matching body codecs on both peers; framing is always outermost.

    Lengths are bounded before reading bodies. Codec and rule failures on
    malformed input should raise ProtocolError. Codecs must be stateless or
    safe to share across connections.
    """

    command_codec: Codec[Command, bytes] = COMMANDS
    frame_codec: Codec[Frame, bytes] = FRAMES
    framing: LengthPrefix = field(default_factory=lambda: LengthPrefix(MAX_FRAME_LEN))

    @cached_property
    def commands(self) -> CodecChain[Command, bytes]:
        return CodecChain.of(self.command_codec) | self.framing

    @cached_property
    def frames(self) -> CodecChain[Frame, bytes]:
        return CodecChain.of(self.frame_codec) | self.framing

    async def _read_bodies(self, reader: asyncio.StreamReader) -> AsyncGenerator[bytes]:
        """Yield frame bodies until EOF; raise if EOF interrupts a frame."""
        decoder = FrameDecoder(self.framing)
        while chunk := await reader.read(READ_SIZE):
            decoder.feed(chunk)
            for body in decoder.bodies():
                yield body
        if decoder.pending:
            raise asyncio.IncompleteReadError(decoder.pending, None)

    async def read_commands(
        self, reader: asyncio.StreamReader
    ) -> AsyncGenerator[Command]:
        async for body in self._read_bodies(reader):
            yield self.command_codec.decode(body)

    async def read_frames(self, reader: asyncio.StreamReader) -> AsyncGenerator[Frame]:
        async for body in self._read_bodies(reader):
            yield self.frame_codec.decode(body)


DEFAULT_PROTOCOL = WireProtocol()
