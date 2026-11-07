from __future__ import annotations

import asyncio
import contextlib

from velis.codec import CodecChain
from velis.core.commands import (
    Command,
    Delivered,
    Err,
    Expire,
    Get,
    Message,
    Nil,
    Ok,
    Publish,
    Reply,
    Set,
    Subscribe,
    Unsubscribe,
    Value,
)
from velis.errors import CommandError, ProtocolError
from velis.net import protocol


class Client:
    """Async client with client-side codecs and serialized requests."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        codec: CodecChain,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._codec = codec
        self._lock = asyncio.Lock()
        self._replies: asyncio.Queue[Reply | None] = asyncio.Queue()
        self._pushes: asyncio.Queue[Message | None] = asyncio.Queue()
        self._closed = False
        self._pump_task = asyncio.create_task(self._pump())

    @classmethod
    async def connect(
        cls, host: str, port: int, codec: CodecChain | None = None
    ) -> Client:
        """Open a TCP connection and start reading replies and server pushes."""
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer, codec or CodecChain())

    async def _pump(self) -> None:
        """Split the incoming stream into replies and server pushes."""
        try:
            while True:
                frame = await protocol.read_frame(self._reader)
                if isinstance(frame, Message):
                    self._pushes.put_nowait(frame)
                else:
                    self._replies.put_nowait(frame)
        except asyncio.IncompleteReadError, ConnectionError, ProtocolError:
            pass
        finally:
            self._mark_closed()

    def _mark_closed(self) -> None:
        """Close the stream and wake readers, including on local shutdown."""
        if self._closed:
            return
        self._closed = True
        self._writer.close()
        self._replies.put_nowait(None)
        self._pushes.put_nowait(None)

    async def _request(self, cmd: Command) -> Reply:
        """Send one command and wait for its reply under the request lock.

        Cancellation after sending closes the stream: replies have no IDs,
        so an abandoned reply cannot safely be reused for another command.
        """
        async with self._lock:
            if self._closed:
                raise ConnectionError("connection closed")
            data = protocol.encode_command(cmd)
            try:
                self._writer.write(data)
                await self._writer.drain()
                reply = await self._replies.get()
            except asyncio.CancelledError, ConnectionError:

                await self.close()
                raise
        if reply is None:
            self._replies.put_nowait(None)
            raise ConnectionError("connection closed")
        if isinstance(reply, Err):
            raise CommandError(reply.message)
        return reply

    async def get(self, key: str) -> bytes | None:
        """Return the decoded value, or None for a missing or expired key."""
        match await self._request(Get(key)):
            case Value(v):
                return self._codec.decode(v)
            case Nil():
                return None
            case other:
                raise ProtocolError(f"unexpected reply to GET: {other!r}")

    async def set(self, key: str, value: bytes) -> None:
        """Encode and store a value, clearing any previous TTL."""
        await self._expect_ok(Set(key, self._codec.encode(value)))

    async def expire(self, key: str, ms: int) -> bool:
        """Set a TTL in milliseconds; return False if the key is missing."""
        match await self._request(Expire(key, ms)):
            case Ok():
                return True
            case Nil():
                return False
            case other:
                raise ProtocolError(f"unexpected reply to EXPIRE: {other!r}")

    async def subscribe(self, channel: str) -> None:
        """Join a channel to receive server pushes."""
        await self._request(Subscribe(channel))

    async def unsubscribe(self, channel: str) -> None:
        """Leave a channel without closing the connection."""
        await self._request(Unsubscribe(channel))

    async def publish(self, channel: str, payload: bytes) -> int:
        """Encode a message and return the number of accepting subscribers."""
        match await self._request(Publish(channel, self._codec.encode(payload))):
            case Delivered(n):
                return n
            case other:
                raise ProtocolError(f"unexpected reply to PUBLISH: {other!r}")

    async def next_message(self) -> Message:
        """Wait for the next message and decode its payload.

        Use asyncio.timeout to bound the wait. Cancelling this wait leaves
        the connection usable. Raise ConnectionError after queued messages
        have been drained and the connection is closed.
        """
        msg = await self._pushes.get()
        if msg is None:
            self._pushes.put_nowait(None)
            raise ConnectionError("connection closed")
        return Message(msg.channel, self._codec.decode(msg.payload))

    async def _expect_ok(self, cmd: Command) -> None:
        reply = await self._request(cmd)
        if not isinstance(reply, Ok):
            raise ProtocolError(f"unexpected reply: {reply!r}")

    async def close(self) -> None:
        """Close the stream, wake waiting callers and stop the reader task."""
        self._mark_closed()
        self._pump_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._pump_task
        with contextlib.suppress(ConnectionError):
            await self._writer.wait_closed()
