"""Transport sessions for serialized commands and asynchronous server messages."""

import asyncio
import contextlib
from typing import Protocol

from velis.core.commands import Command, Err, Message, Reply
from velis.errors import ProtocolError
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol


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
    Construction starts the reader task and requires a running event loop.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        wire: WireProtocol = DEFAULT_PROTOCOL,
        max_pending_messages: int = 1024,
        max_pending_bytes: int = 8 << 20,
    ) -> None:
        if max_pending_messages <= 0 or max_pending_bytes <= 0:
            raise ValueError("pending message limits must be positive")
        self._wire = wire
        self._reader = reader
        self._writer = writer
        self._request_lock = asyncio.Lock()
        self._replies: asyncio.Queue[Reply] = asyncio.Queue(maxsize=1)
        self._awaiting_reply = False
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
        wire: WireProtocol = DEFAULT_PROTOCOL,
        max_pending_messages: int = 1024,
        max_pending_bytes: int = 8 << 20,
    ) -> TcpSession:
        """Open a TCP connection and start reading replies and server pushes."""
        reader, writer = await asyncio.open_connection(host, port)
        try:
            return cls(
                reader,
                writer,
                wire=wire,
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
            while not self._closed:
                frame = await self._wire.read_frame(self._reader)
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
            self._begin_close("pending message buffer limit exceeded")
            return
        self._messages.put_nowait(message)
        self._queued_bytes += size

    def _resolve_reply(self, reply: Reply) -> None:
        """Complete the active request, rejecting unsolicited or duplicate replies."""
        if not self._awaiting_reply or not self._replies.empty():
            if not self._awaiting_reply and isinstance(reply, Err):
                self._begin_close(reply.message)
                return
            raise ProtocolError("reply received without a pending request")
        self._replies.put_nowait(reply)

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
        self._replies.shutdown()
        self._messages.shutdown()

    async def request(self, command: Command) -> Reply:
        """Send one command and wait for its reply under the request lock.

        Cancellation after sending closes the stream: replies have no IDs,
        so an abandoned reply cannot safely be reused for another command.
        """
        async with self._request_lock:
            if self._closed:
                raise ConnectionError(self._close_reason)
            encoded_command = self._wire.commands.encode(command)
            self._awaiting_reply = True
            try:
                self._writer.write(encoded_command)
                await self._writer.drain()
                try:
                    return await self._replies.get()
                except asyncio.QueueShutDown:
                    raise ConnectionError(self._close_reason) from None
            except asyncio.CancelledError, ConnectionError:
                await self.close()
                raise
            finally:
                self._awaiting_reply = False

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
