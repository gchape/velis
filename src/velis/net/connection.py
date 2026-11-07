from __future__ import annotations

import asyncio

from velis.net import protocol


class Connection:
    """One client socket. Implements core.broker.Subscriber."""

    __slots__ = ("_max_buffer", "reader", "writer")

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        max_buffer: int,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self._max_buffer = max_buffer

    def send(self, channel: str, payload: bytes) -> bool:
        """Queue a push; reject disconnected, failed or overloaded transports.

        Abort slow connections immediately instead of waiting to flush them.
        """
        if self.writer.is_closing():
            return False
        data = protocol.encode_message(channel, payload)
        try:
            self.writer.write(data)
        except ConnectionError:
            self.writer.transport.abort()
            return False
        if self.writer.transport.get_write_buffer_size() > self._max_buffer:
            self.writer.transport.abort()
            return False
        return True

    def write(self, data: bytes) -> None:
        """Queue encoded bytes for the client connection."""
        self.writer.write(data)

    async def drain(self) -> None:
        """Wait for the transport to allow further writes."""
        await self.writer.drain()

    def close(self) -> None:
        """Begin closing the client connection."""
        self.writer.close()
