"""Transport sessions for ordered commands and asynchronous server messages."""

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from typing import Protocol

from velis.core.commands import Command, Err, Message, Reply
from velis.errors import ProtocolError
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol
from velis.validation import require_positive


@dataclass(frozen=True, slots=True)
class SessionLimits:
    """Bounds on in-flight requests and on messages the caller has not read."""

    max_pending_requests: int = 128
    max_pending_messages: int = 1024
    max_pending_bytes: int = 8 << 20

    def __post_init__(self) -> None:
        require_positive("max_pending_requests", self.max_pending_requests)
        require_positive("max_pending_messages", self.max_pending_messages)
        require_positive("max_pending_bytes", self.max_pending_bytes)


DEFAULT_LIMITS = SessionLimits()


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
    """Pipeline TCP requests and buffer server messages.

    The server replies in the order it receives commands, so each reply
    belongs to the oldest unanswered request. Bound in-flight requests,
    message count and channel/payload bytes. Closing wakes waiters and lets
    queued messages drain before reporting closure. Construction starts the
    reader task and requires a running event loop.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        wire: WireProtocol = DEFAULT_PROTOCOL,
        limits: SessionLimits = DEFAULT_LIMITS,
    ) -> None:
        self._wire = wire
        self._reader = reader
        self._writer = writer
        self._slots = asyncio.Semaphore(limits.max_pending_requests)
        self._pending: deque[asyncio.Future[Reply]] = deque()
        self._max_pending_bytes = limits.max_pending_bytes
        self._queued_bytes = 0
        self._messages: asyncio.Queue[Message] = asyncio.Queue(
            maxsize=limits.max_pending_messages
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
        limits: SessionLimits = DEFAULT_LIMITS,
    ) -> TcpSession:
        """Open a TCP connection and start reading replies and server pushes."""
        reader, writer = await asyncio.open_connection(host, port)
        try:
            return cls(reader, writer, wire=wire, limits=limits)
        except Exception:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            raise

    async def _receive_frames(self) -> None:
        """Split the incoming stream into replies and server pushes."""
        try:
            async with contextlib.aclosing(
                self._wire.read_frames(self._reader)
            ) as frames:
                async for frame in frames:
                    if isinstance(frame, Message):
                        self._enqueue_message(frame)
                    else:
                        self._resolve_reply(frame)
                    if self._closed:
                        break
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
        """Resolve the oldest request or discard its cancelled future's reply.

        An unsolicited Err closes the session with the server's rejection reason.
        """
        if not self._pending:
            if isinstance(reply, Err):
                self._begin_close(reply.message)
                return
            raise ProtocolError("reply received without a pending request")
        waiter = self._pending.popleft()
        self._slots.release()
        if not waiter.done():
            waiter.set_result(reply)

    @staticmethod
    def _message_size(message: Message) -> int:
        """Count queued channel and payload bytes before codec decoding."""
        return len(message.channel.encode()) + len(message.payload)

    def _begin_close(self, reason: str = "connection closed") -> None:
        """Close and wake callers, preserving queued messages for draining.

        Mark failed futures as retrieved so abandoned callers leave no warnings.
        """
        if self._closed:
            return
        self._closed = True
        self._close_reason = reason
        self._writer.close()
        for waiter in self._pending:
            self._slots.release()
            if not waiter.done():
                waiter.set_exception(ConnectionError(reason))
                waiter.exception()
        self._pending.clear()
        self._messages.shutdown()

    async def request(self, command: Command) -> Reply:
        """Send a command and await the reply that answers it.

        Cancellation abandons the reply without closing the stream. A sent
        request keeps its slot until its reply arrives, which bounds the
        bookkeeping for cancelled requests even if the peer stops responding.
        Capacity is acquired before wire encoding. Queueing the future and writing
        are synchronous so FIFO order matches wire order. Cancellation during drain
        also retains the future's position while discarding its eventual result.
        """
        if self._closed:
            raise ConnectionError(self._close_reason)
        await self._slots.acquire()
        waiter: asyncio.Future[Reply] | None = None
        try:
            if self._closed:
                raise ConnectionError(self._close_reason)
            encoded = self._wire.commands.encode(command)
            waiter = asyncio.get_running_loop().create_future()
            self._pending.append(waiter)
            self._writer.write(encoded)
            del encoded
            await self._writer.drain()
            return await waiter
        except ConnectionError:
            await self.close()
            raise
        finally:
            if waiter is None:
                self._slots.release()
            elif not waiter.done():
                waiter.cancel()

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
