"""Composable command/message codecs and bounded TCP message framing.

Default messages use a u32 body length followed by the encoded body. Bodies
use a text tag and ordered fields; tagged tables map each tag to its dataclass
variant and explicit field layouts. The server answers commands in order, so
replies carry no request identifiers.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass, field
from functools import cached_property, partial

from velis.codec import (
    U32,
    U64,
    UTF8,
    Choice,
    Codec,
    CompositeCodec,
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

MAX_FIELD_BYTES = 1 << 20
MAX_FRAME_BODY_BYTES = 2 * MAX_FIELD_BYTES

type ServerFrame = Reply | Message

BYTES = LengthPrefixedField(Identity(), MAX_FIELD_BYTES, prefix=U32)
TEXT = LengthPrefixedField(UTF8(), MAX_FIELD_BYTES, prefix=U32)


def _check_text(value: str, *, what: str, max_chars: int) -> str | None:
    if not value:
        return f"{what} must not be empty"
    if len(value) > max_chars:
        return f"{what} exceeds {max_chars} characters"
    return None


def _text_field(what: str, max_chars: int) -> LengthPrefixedField[str]:
    """Bound text by characters and allow up to four UTF-8 bytes per character."""
    rule = Validate[str](partial(_check_text, what=what, max_chars=max_chars))
    return LengthPrefixedField(
        CompositeCodec.of(rule) | UTF8(),
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
        "EXPIRE": Record(Expire, KEY, U64),
        "DEL": Record(Delete, KEY),
        "TTL": Record(TTL, KEY),
        "PERSIST": Record(Persist, KEY),
        "SUBSCRIBE": Record(Subscribe, CHANNEL),
        "UNSUBSCRIBE": Record(Unsubscribe, CHANNEL),
        "PUBLISH": Record(Publish, CHANNEL, BYTES),
    },
)

SERVER_MESSAGES: Choice[ServerFrame] = Choice(
    "server message",
    {
        "OK": Record(Ok),
        "NIL": Record(Nil),
        "VALUE": Record(Value, BYTES),
        "ERR": Record(Err, TEXT),
        "SUBSCRIBED": Record(Subscribed, TEXT),
        "UNSUBSCRIBED": Record(Unsubscribed, TEXT),
        "DELIVERED": Record(Delivered, U32),
        "TTL_VALUE": Record(TTLValue, U64),
        "PERSISTENT": Record(Persistent),
        "MESSAGE": Record(Message, TEXT, BYTES),
    },
)


class PushEncoder:
    """Encode a push once for consecutive deliveries of the same object.

    Retain only the latest push and its bytes. Equal but distinct pushes are
    encoded separately; encoding failures propagate without replacing the cache.
    """

    def __init__(self, encode: Callable[[Message], bytes]) -> None:
        self._encode = encode
        self._last: tuple[Message, bytes] | None = None

    def __call__(self, message: Message) -> bytes:
        last = self._last
        if last is None or last[0] is not message:
            self._last = last = (message, self._encode(message))
        return last[1]


@dataclass(frozen=True)
class WireProtocol:
    """Use matching body codecs on both peers; framing is always outermost.

    Lengths are bounded before reading bodies. Codec and rule failures on
    malformed input should raise ProtocolError. Codecs must be stateless or
    safe to share across connections.
    """

    command_codec: Codec[Command, bytes] = COMMANDS
    message_codec: Codec[ServerFrame, bytes] = SERVER_MESSAGES
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
    def framed_messages(self) -> CompositeCodec[ServerFrame, bytes]:
        """Encode and decode complete server messages, including replies and pushes."""
        return CompositeCodec.of(self.message_codec) | self.framing

    @cached_property
    def push_encoder(self) -> PushEncoder:
        """Share the latest push encoding across connections using this instance."""
        return PushEncoder(self.framed_messages.encode)

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
    ) -> AsyncGenerator[ServerFrame]:
        """Yield decoded replies and pushes until the server disconnects cleanly."""
        return self._read(reader, self.message_codec)


DEFAULT_PROTOCOL = WireProtocol()
