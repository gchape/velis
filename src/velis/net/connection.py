from __future__ import annotations

import asyncio
import contextlib

from velis.core.commands import Command, Message, Reply
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol


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

    async def read_command(self) -> Command:
        return await self._wire.read_command(self._reader)

    def write_reply(self, reply: Reply) -> None:
        """Validate and encode the complete reply before writing any bytes."""
        self._writer.write(self._wire.frames.encode(reply))

    def send(self, channel: str, payload: bytes) -> bool:
        """Queue a push; reject disconnected, failed or overloaded transports.

        Abort slow connections immediately instead of waiting to flush them.
        """
        if self._writer.is_closing():
            return False
        data = self._wire.frames.encode(Message(channel, payload))
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
