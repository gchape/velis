"""Owned server-side streams and synchronous publication delivery."""

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from typing import Self

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
        self._writer.close()
        with contextlib.suppress(OSError):
            await self._writer.wait_closed()

    def read_commands(self) -> AsyncGenerator[Command]:
        """Yield commands until the peer disconnects cleanly."""
        return self._wire.read_commands(self._reader)

    async def write_reply(self, reply: Reply) -> None:
        """Encode a complete reply and let the stream apply write flow control."""
        self._writer.write(self._wire.framed_messages.encode(reply))
        await self._writer.drain()

    def send(self, message: Message) -> bool:
        """Queue a push and return whether the transport accepted it.

        Closed transports reject without writing. Protocol and OS errors, or a
        write buffer strictly above the limit, abort the transport and return
        False; unexpected exceptions propagate. Acceptance does not confirm that
        the subscriber read the push, and this method never waits for socket I/O.
        """
        transport = self._writer.transport
        if transport.is_closing():
            return False
        try:
            self._writer.write(self._wire.push_encoder(message))
        except (ProtocolError, OSError) as exc:
            if isinstance(exc, ProtocolError):
                log.warning(
                    "dropping subscriber: cannot encode push on %r: %s",
                    message.channel,
                    exc,
                )
            transport.abort()
            return False
        if (
            transport.is_closing()
            or transport.get_write_buffer_size() > self._max_buffer
        ):
            transport.abort()
            return False
        return True
