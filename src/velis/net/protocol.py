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
    UTF8,
    Choice,
    Codec,
    CompositeCodec,
    FixedLengthField,
    Identity,
    LengthPrefixedField,
    Record,
    Validate,
)
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

MAX_FIELD_BYTES = 1 << 20
MAX_FRAME_BODY_BYTES = 2 * MAX_FIELD_BYTES

type ServerMessage = Reply | Message

BYTES = LengthPrefixedField(Identity(), MAX_FIELD_BYTES, prefix=U32)
TEXT = LengthPrefixedField(UTF8(), MAX_FIELD_BYTES, prefix=U32)


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
        CompositeCodec.of(Validate(_text_rule(what, max_chars))) | UTF8(),
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
        "TTL": Record(TTL, KEY),
        "PERSIST": Record(Persist, KEY),
        "SUBSCRIBE": Record(Subscribe, CHANNEL),
        "UNSUBSCRIBE": Record(Unsubscribe, CHANNEL),
        "PUBLISH": Record(Publish, CHANNEL, BYTES),
    },
)

SERVER_MESSAGES: Choice[ServerMessage] = Choice(
    "frame",
    {
        "OK": Record(Ok),
        "NIL": Record(Nil),
        "VALUE": Record(Value, BYTES),
        "ERR": Record(Err, TEXT),
        "SUBSCRIBED": Record(Subscribed, TEXT),
        "UNSUBSCRIBED": Record(Unsubscribed, TEXT),
        "DELIVERED": Record(Delivered, FixedLengthField(U32, 4)),
        "TTL_VALUE": Record(TTLValue, FixedLengthField(U64, 8)),
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
    message_codec: Codec[ServerMessage, bytes] = SERVER_MESSAGES
    framing: LengthPrefixedField[bytes] = field(
        default_factory=lambda: LengthPrefixedField(
            Identity(), MAX_FRAME_BODY_BYTES, prefix=U32
        )
    )

    @cached_property
    def framed_commands(self) -> CompositeCodec[Command, bytes]:
        """Encode and decode whole framed commands."""
        return CompositeCodec.of(self.command_codec) | self.framing

    @cached_property
    def framed_messages(self) -> CompositeCodec[ServerMessage, bytes]:
        """Encode and decode whole framed replies and pushes."""
        return CompositeCodec.of(self.message_codec) | self.framing

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

    def read_messages(
        self, reader: asyncio.StreamReader
    ) -> AsyncGenerator[ServerMessage]:
        """Yield decoded replies and pushes until the server disconnects cleanly."""
        return self._read(reader, self.message_codec)


DEFAULT_PROTOCOL = WireProtocol()
