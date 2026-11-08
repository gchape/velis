import asyncio
import contextlib
import logging
from collections.abc import Generator
from contextvars import ContextVar

from velis.core.commands import Message, Reply
from velis.errors import ProtocolError
from velis.net.protocol import DEFAULT_PROTOCOL, Request, Response, WireProtocol

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

    async def read_command(self) -> Request:
        return await self._wire.read_command(self._reader)

    def write_reply(self, reply: Reply, *, request_id: int) -> None:
        """Validate and encode the complete reply before writing any bytes."""
        self._writer.write(self._wire.frames.encode(Response(request_id, reply)))

    def _encode_push(self, channel: str, payload: bytes) -> bytes:
        """Encode once per wire protocol within the current dispatch scope."""
        encodings = _push_encodings.get()
        if encodings is None:
            return self._wire.frames.encode(Message(channel, payload))
        key = (id(self._wire), channel, payload)
        data = encodings.get(key)
        if data is None:
            data = self._wire.frames.encode(Message(channel, payload))
            encodings[key] = data
        return data

    def send(self, channel: str, payload: bytes) -> bool:
        """Queue a push; reject disconnected, failed or overloaded transports.

        Abort slow connections immediately instead of waiting to flush them.
        """
        if self._writer.is_closing():
            return False
        try:
            data = self._encode_push(channel, payload)
        except ProtocolError:
            log.warning("dropping subscriber: cannot encode push")
            self._writer.transport.abort()
            return False
        try:
            self._writer.write(data)
        except ConnectionError:
            self._writer.transport.abort()
            return False
        if self._writer.transport.get_write_buffer_size() > self._max_buffer:
            self._writer.transport.abort()
            return False
        return True

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
