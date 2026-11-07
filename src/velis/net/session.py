"""Transport sessions for serialized commands and asynchronous server messages."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Protocol

from velis.core.commands import Command, Message, Reply
from velis.errors import ProtocolError
from velis.net import protocol


class Session(Protocol):
    """Exchange encoded application data independently of client codecs.

    Implementations own request ordering, message delivery and shutdown.
    Replies include server errors; the client translates them for callers.
    """

    async def request(self, command: Command) -> Reply:
        """Exchange one command for its corresponding reply."""
        ...

    async def next_message(self) -> Message:
        """Receive an encoded publication."""
        ...

    async def close(self) -> None:
        """Release resources and wake waiting operations; safe to repeat."""
        ...


class TcpSession:
    """Serialize TCP requests and bound unread server messages.

    Limit both message count and channel/payload bytes. Closing wakes waiting
    readers and lets already queued messages drain before reporting closure.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        max_pending_messages: int = 1024,
        max_pending_bytes: int = 8 << 20,
    ) -> None:
        if max_pending_messages <= 0 or max_pending_bytes <= 0:
            raise ValueError("pending message limits must be positive")
        self._reader = reader
        self._writer = writer
        self._request_lock = asyncio.Lock()
        self._pending_reply: asyncio.Future[Reply] | None = None
        self._max_pending_bytes = max_pending_bytes
        self._queued_bytes = 0
        self._messages: asyncio.Queue[Message] = asyncio.Queue(
            maxsize=max_pending_messages
        )
        self._closed = False
        self._close_reason = "connection closed"
        self._reader_task = asyncio.create_task(self._receive_frames())

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        max_pending_messages: int = 1024,
        max_pending_bytes: int = 8 << 20,
    ) -> TcpSession:
        """Open a TCP connection and start reading replies and server pushes."""
        reader, writer = await asyncio.open_connection(host, port)
        try:
            return cls(
                reader,
                writer,
                max_pending_messages=max_pending_messages,
                max_pending_bytes=max_pending_bytes,
            )
        except Exception:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            raise

    async def _receive_frames(self) -> None:
        """Split the incoming stream into replies and server pushes."""
        try:
            while True:
                frame = await protocol.read_frame(self._reader)
                if isinstance(frame, Message):
                    self._enqueue_message(frame)
                else:
                    self._resolve_reply(frame)
        except ProtocolError as exc:
            self._begin_close(str(exc))
        except asyncio.IncompleteReadError, ConnectionError:
            pass
        finally:
            self._begin_close()

    def _enqueue_message(self, message: Message) -> None:
        """Buffer a publication within the message count and byte limits."""
        size = self._message_size(message)
        if self._messages.full() or self._queued_bytes + size > self._max_pending_bytes:
            raise ProtocolError("pending message buffer limit exceeded")
        self._messages.put_nowait(message)
        self._queued_bytes += size

    def _resolve_reply(self, reply: Reply) -> None:
        """Complete the active request, rejecting unsolicited or duplicate replies."""
        if self._pending_reply is None or self._pending_reply.done():
            raise ProtocolError("reply received without a pending request")
        self._pending_reply.set_result(reply)

    @staticmethod
    def _message_size(message: Message) -> int:
        """Count queued channel and payload bytes before codec decoding."""
        return len(message.channel.encode()) + len(message.payload)

    def _begin_close(self, reason: str = "connection closed") -> None:
        """Close the stream and wake readers, including on local shutdown."""
        if self._closed:
            return
        self._closed = True
        self._close_reason = reason
        self._writer.close()
        if self._pending_reply is not None and not self._pending_reply.done():
            self._pending_reply.set_exception(ConnectionError(reason))
        self._messages.shutdown()

    async def request(self, command: Command) -> Reply:
        """Send one command and wait for its reply under the request lock.

        Cancellation after sending closes the stream: replies have no IDs,
        so an abandoned reply cannot safely be reused for another command.
        """
        async with self._request_lock:
            if self._closed:
                raise ConnectionError(self._close_reason)
            encoded_command = protocol.encode_command(command)
            reply_future: asyncio.Future[Reply] = (
                asyncio.get_running_loop().create_future()
            )
            self._pending_reply = reply_future
            try:
                self._writer.write(encoded_command)
                await self._writer.drain()
                reply = await reply_future
            except asyncio.CancelledError, ConnectionError:
                await self.close()
                raise
            finally:
                self._pending_reply = None
                if not reply_future.done():
                    reply_future.cancel()
                elif not reply_future.cancelled():
                    reply_future.exception()
        return reply

    async def next_message(self) -> Message:
        """Wait for the next encoded message.

        Use asyncio.timeout to bound the wait. Cancelling this wait leaves
        the connection usable. Raise ConnectionError after queued messages
        have been drained and the connection is closed.
        """
        try:
            message = await self._messages.get()
        except asyncio.QueueShutDown:
            raise ConnectionError(self._close_reason) from None
        self._queued_bytes -= self._message_size(message)
        return message

    async def close(self) -> None:
        """Close the stream, wake waiting callers and stop the reader task."""
        self._begin_close()
        self._reader_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._reader_task
        with contextlib.suppress(ConnectionError):
            await self._writer.wait_closed()
