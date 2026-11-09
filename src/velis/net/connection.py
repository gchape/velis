"""Server-side connection: wire I/O plus push buffering for one subscriber."""

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator, Generator
from contextvars import ContextVar

from velis.core.messages import Command, Message, Reply
from velis.errors import ProtocolError
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol

log = logging.getLogger(__name__)

_push_encodings: ContextVar[dict[tuple[int, str, bytes], bytes] | None] = ContextVar(
    "push_encodings", default=None
)


@contextlib.contextmanager
def share_message_encodings() -> Generator[None]:
    """Reuse each wire encoding during one synchronous command dispatch.

    Connections using different WireProtocol instances have separate encodings.
    Reset after dispatch so no payload remains cached between publications.
    """
    token = _push_encodings.set({})
    try:
        yield
    finally:
        _push_encodings.reset(token)


class Connection:
    """Server-side wire I/O and push buffering for one subscriber socket."""

    __slots__ = ("_max_buffer", "_reader", "_wire", "_writer")

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        max_buffer: int,
        *,
        wire: WireProtocol = DEFAULT_PROTOCOL,
    ) -> None:
        self._wire = wire
        self._reader = reader
        self._writer = writer
        self._max_buffer = max_buffer

    def read_commands(self) -> AsyncGenerator[Command]:
        """Yield commands until the peer disconnects cleanly."""
        return self._wire.read_commands(self._reader)

    def write_reply(self, reply: Reply) -> None:
        """Validate and encode the complete reply before writing any bytes."""
        self._writer.write(self._wire.framed_messages.encode(reply))

    def _encode(self, channel: str, payload: bytes) -> bytes:
        return self._wire.framed_messages.encode(Message(channel, payload))

    def _encode_push(self, channel: str, payload: bytes) -> bytes:
        """Encode once per wire protocol within the current dispatch scope."""
        encodings = _push_encodings.get()
        if encodings is None:
            return self._encode(channel, payload)
        key = (id(self._wire), channel, payload)
        if (data := encodings.get(key)) is None:
            data = encodings[key] = self._encode(channel, payload)
        return data

    def send(self, channel: str, payload: bytes) -> bool:
        """Queue a push; reject disconnected, failed or overloaded transports.

        Abort slow connections immediately instead of waiting to flush them.
        """
        transport = self._writer.transport
        if transport.is_closing():
            return False
        try:
            data = self._encode_push(channel, payload)
        except ProtocolError as exc:
            log.warning(
                "dropping subscriber: cannot encode push on %r: %s", channel, exc
            )
        else:
            self._writer.write(data)
            # write() never raises for a lost connection: a failed send
            # force-closes the transport, so check afterwards.
            if (
                not transport.is_closing()
                and transport.get_write_buffer_size() <= self._max_buffer
            ):
                return True
        transport.abort()
        return False

    async def drain(self) -> None:
        """Wait for the transport to allow further writes."""
        await self._writer.drain()

    def close(self) -> None:
        """Begin closing the client connection."""
        self._writer.close()

    async def wait_closed(self) -> None:
        """Finish socket cleanup even when the peer has already disconnected."""
        with contextlib.suppress(ConnectionError):
            await self._writer.wait_closed()
