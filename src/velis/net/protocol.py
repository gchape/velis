"""Composable command/frame codecs and bounded TCP message framing.

Default messages use a u32 body length followed by the encoded body. Bodies
use a text tag and ordered fields; tagged tables map each tag to its dataclass
variant and explicit field layouts. The server answers commands in order, so
replies carry no request identifiers.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass, field
from functools import cached_property

from velis.codec import (
    U32,
    U64,
    Choice,
    Codec,
    CompositeCodec,
    FixedLengthField,
    Identity,
    LengthPrefixedField,
    Record,
    Utf8,
    Validate,
)
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

type Frame = Reply | Message

BYTES = LengthPrefixedField(Identity(), MAX_LEN, prefix=U32)
TEXT = LengthPrefixedField(Utf8(), MAX_LEN, prefix=U32)


def _text_rule(what: str, max_chars: int) -> Callable[[str], None]:
    def check(value: str) -> None:
        if not value:
            raise ProtocolError(f"{what} must not be empty")
        if len(value) > max_chars:
            raise ProtocolError(f"{what} exceeds {max_chars} characters")

    return check


def _text_field(what: str, max_chars: int) -> LengthPrefixedField[str]:
    """Bound text by characters and allow up to four UTF-8 bytes per character."""
    return LengthPrefixedField(
        CompositeCodec.of(Validate(_text_rule(what, max_chars))) | Utf8(),
        max_chars * 4,
        prefix=U32,
    )


KEY = _text_field("key", 512)
CHANNEL = _text_field("channel", 256)

COMMANDS: Choice[Command] = Choice(
    "command",
    {
        "GET": Record(Get, KEY),
        "SET": Record(Set, KEY, BYTES),
        "EXPIRE": Record(Expire, KEY, FixedLengthField(U64, 8)),
        "DEL": Record(Delete, KEY),
        "TTL": Record(Ttl, KEY),
        "PERSIST": Record(Persist, KEY),
        "SUBSCRIBE": Record(Subscribe, CHANNEL),
        "UNSUBSCRIBE": Record(Unsubscribe, CHANNEL),
        "PUBLISH": Record(Publish, CHANNEL, BYTES),
    },
)

FRAMES: Choice[Frame] = Choice(
    "frame",
    {
        "OK": Record(Ok),
        "NIL": Record(Nil),
        "VALUE": Record(Value, BYTES),
        "ERR": Record(Err, TEXT),
        "SUBSCRIBED": Record(Subscribed, TEXT),
        "UNSUBSCRIBED": Record(Unsubscribed, TEXT),
        "DELIVERED": Record(Delivered, FixedLengthField(U32, 4)),
        "TTL_VALUE": Record(TtlValue, FixedLengthField(U64, 8)),
        "PERSISTENT": Record(Persistent),
        "MESSAGE": Record(Message, TEXT, BYTES),
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
    framing: LengthPrefixedField[bytes] = field(
        default_factory=lambda: LengthPrefixedField(
            Identity(), MAX_FRAME_LEN, prefix=U32
        )
    )

    @cached_property
    def commands(self) -> CompositeCodec[Command, bytes]:
        """Encode and decode whole framed commands."""
        return CompositeCodec.of(self.command_codec) | self.framing

    @cached_property
    def frames(self) -> CompositeCodec[Frame, bytes]:
        """Encode and decode whole framed replies and pushes."""
        return CompositeCodec.of(self.frame_codec) | self.framing

    async def _read_body(self, reader: asyncio.StreamReader) -> bytes | None:
        """Return one frame body, or None on EOF at a frame boundary.

        Raise IncompleteReadError if EOF interrupts a frame.
        """
        try:
            header = await reader.readexactly(self.framing.prefix.length)
        except asyncio.IncompleteReadError as exc:
            if exc.partial:
                raise
            return None
        length = self.framing.prefix.decode(header)
        self.framing.check_length(length)
        return self.framing.codec.decode(await reader.readexactly(length))

    async def _read[T](
        self, reader: asyncio.StreamReader, codec: Codec[T, bytes]
    ) -> AsyncGenerator[T]:
        while (body := await self._read_body(reader)) is not None:
            yield codec.decode(body)

    def read_commands(self, reader: asyncio.StreamReader) -> AsyncGenerator[Command]:
        """Yield decoded commands until the peer disconnects cleanly."""
        return self._read(reader, self.command_codec)

    def read_frames(self, reader: asyncio.StreamReader) -> AsyncGenerator[Frame]:
        """Yield decoded replies and pushes until the server disconnects cleanly."""
        return self._read(reader, self.frame_codec)


DEFAULT_PROTOCOL = WireProtocol()
