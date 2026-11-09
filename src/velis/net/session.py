"""Transport sessions for ordered commands and asynchronous server messages."""

import asyncio
import contextlib
from typing import Protocol, Self

from velis.config import DEFAULT_SESSION_LIMITS, SessionLimits
from velis.core.messages import Command, Err, Message, Reply
from velis.errors import ProtocolError
from velis.net import protocol

_CLOSED = "connection closed"


class _MessageBuffer:
    """Hold unread server messages within a count limit and a byte limit.

    Each entry carries its size, measured once on arrival. Closing lets queued
    messages drain before readers see the end.
    """

    def __init__(self, max_messages: int, max_bytes: int) -> None:
        self._queue: asyncio.Queue[tuple[int, Message]] = asyncio.Queue(max_messages)
        self._max_bytes = max_bytes
        self._bytes = 0

    def offer(self, message: Message) -> bool:
        """Queue a message; return False if a limit would be exceeded."""
        size = len(message.channel.encode()) + len(message.payload)
        if self._bytes + size > self._max_bytes:
            return False
        try:
            self._queue.put_nowait((size, message))
        except asyncio.QueueFull:
            return False
        self._bytes += size
        return True

    async def take(self) -> Message:
        """Wait for a message; raise QueueShutDown once closed and drained."""
        size, message = await self._queue.get()
        self._bytes -= size
        return message

    def close(self) -> None:
        self._queue.shutdown()


class Session(Protocol):
    """Exchange typed commands, replies and publications over an owned transport.

    Implementations own request ordering, message delivery and shutdown.
    Replies include server errors; the client translates them for callers.
    """

    async def request(self, command: Command) -> Reply:
        """Exchange one command for its corresponding reply."""
        ...

    async def next_message(self) -> Message:
        """Receive a Message containing its channel and opaque payload bytes."""
        ...

    async def close(self) -> None:
        """Release resources and wake waiting operations; safe to repeat."""
        ...


class TCPSession:
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
        limits: SessionLimits = DEFAULT_SESSION_LIMITS,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._pending: asyncio.Queue[asyncio.Future[Reply]] = asyncio.Queue(
            limits.max_pending_requests
        )
        self._messages = _MessageBuffer(
            limits.max_pending_messages, limits.max_pending_bytes
        )
        self._close_reason: str | None = None
        self._reader_task = asyncio.create_task(self._receive_messages())

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        limits: SessionLimits = DEFAULT_SESSION_LIMITS,
    ) -> Self:
        """Open a TCP connection and start reading replies and server pushes."""
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer, limits=limits)

    async def _receive_messages(self) -> None:
        """Route replies and pushes until closure or a transport/protocol error.

        EOF and OSError close normally; protocol errors preserve their reason.
        Unexpected reader failures remain available to close() after cleanup.
        """
        try:
            while self._close_reason is None:
                body = await protocol.read_frame(self._reader)
                if body is None:
                    break
                self._route(protocol.SERVER_MESSAGES.decode(body))
        except ProtocolError as exc:
            self._begin_close(str(exc))
        except asyncio.IncompleteReadError, OSError:
            pass
        finally:
            self._begin_close()

    def _route(self, message: protocol.ServerFrame) -> None:
        if isinstance(message, Message):
            if not self._messages.offer(message):
                self._begin_close("pending message buffer limit exceeded")
        else:
            self._resolve_reply(message)

    def _resolve_reply(self, reply: Reply) -> None:
        """Resolve the oldest request or discard its cancelled future's reply.

        An unsolicited Err closes the session with the server's rejection reason.
        """
        try:
            waiter = self._pending.get_nowait()
        except asyncio.QueueEmpty:
            if isinstance(reply, Err):
                self._begin_close(reply.message)
                return
            raise ProtocolError("reply received without a pending request") from None
        if not waiter.done():
            waiter.set_result(reply)

    def _begin_close(self, reason: str = _CLOSED) -> None:
        """Close and wake callers, preserving queued messages for draining.

        Repeated calls retain the first close reason and do no further work.
        Mark failed futures as retrieved so abandoned callers leave no warnings.
        """
        if self._close_reason is not None:
            return
        self._close_reason = reason
        self._writer.close()
        while not self._pending.empty():
            waiter = self._pending.get_nowait()
            if not waiter.done():
                waiter.set_exception(ConnectionError(reason))
                waiter.exception()
        self._pending.shutdown()
        self._messages.close()

    async def request(self, command: Command) -> Reply:
        """Send a command and await the reply that answers it.

        Encoding happens first, so an invalid command never takes a slot. A sent
        request keeps its slot until its reply arrives or the session closes,
        bounding bookkeeping for cancelled requests if the peer stops replying.
        Cancellation abandons the reply without closing the stream.
        Other I/O failures close the session to preserve reply ordering when
        a write leaves an unsent waiter or a partial frame.
        """
        frame = protocol.encode_command(command)
        waiter: asyncio.Future[Reply] = asyncio.get_running_loop().create_future()
        try:
            await self._pending.put(waiter)
            self._writer.write(frame)
            await self._writer.drain()
            return await waiter
        except asyncio.QueueShutDown as exc:
            raise ConnectionError(self._close_reason) from exc
        except Exception:
            await self.close()
            raise
        finally:
            waiter.cancel()

    async def next_message(self) -> Message:
        """Wait for the next Message with its original payload bytes.

        Use asyncio.timeout to bound the wait. Cancelling this wait leaves
        the connection usable. Raise ConnectionError after queued messages
        have been drained and the connection is closed.
        """
        try:
            return await self._messages.take()
        except asyncio.QueueShutDown as exc:
            raise ConnectionError(self._close_reason) from exc

    async def close(self) -> None:
        """Close the stream, wake waiting callers and stop the reader task.

        Safe to repeat. Cancelling the caller still propagates, and an
        unexpected reader failure is raised after cleanup completes.
        Expected OSError failures while closing the transport are suppressed.
        """
        self._begin_close()
        self._reader_task.cancel()
        await asyncio.wait((self._reader_task,))
        with contextlib.suppress(OSError):
            await self._writer.wait_closed()
        if not self._reader_task.cancelled():
            self._reader_task.result()
