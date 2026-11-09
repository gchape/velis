"""Owned server-side streams and synchronous publication delivery."""

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from typing import Self

from velis.core.broker import Publication
from velis.core.messages import Command, Message, Reply
from velis.errors import ProtocolError
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol

log = logging.getLogger(__name__)


class Connection:
    """Own wire I/O and bounded push buffering for one subscriber socket."""

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

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: object, *_: object) -> None:
        self.close()
        await self.wait_closed()

    def read_commands(self) -> AsyncGenerator[Command]:
        """Yield commands until the peer disconnects cleanly."""
        return self._wire.read_commands(self._reader)

    def write_reply(self, reply: Reply) -> None:
        """Validate and encode the complete reply before writing any bytes."""
        self._writer.write(self._wire.framed_messages.encode(reply))

    def _encode(self, channel: str, payload: bytes) -> bytes:
        return self._wire.framed_messages.encode(Message(channel, payload))

    def _abort(self) -> bool:
        self._writer.transport.abort()
        return False

    def _overloaded(self) -> bool:
        transport = self._writer.transport
        return (
            transport.is_closing()
            or transport.get_write_buffer_size() > self._max_buffer
        )

    def send(self, publication: Publication) -> bool:
        """Queue a cached publication or abort an overloaded or failed transport."""
        if self._writer.transport.is_closing():
            return False
        try:
            data = publication.encoded_by(self._wire, self._encode)
        except ProtocolError as exc:
            log.warning(
                "dropping subscriber: cannot encode push on %r: %s",
                publication.channel,
                exc,
            )
            return self._abort()
        try:
            self._writer.write(data)
        except OSError:
            return self._abort()
        if self._overloaded():
            return self._abort()
        return True

    async def drain(self) -> None:
        """Wait for the transport to allow further writes."""
        await self._writer.drain()

    def close(self) -> None:
        """Begin closing the client connection."""
        self._writer.close()

    async def wait_closed(self) -> None:
        """Finish socket cleanup even when the peer has already disconnected."""
        with contextlib.suppress(OSError):
            await self._writer.wait_closed()
