"""Transport sessions for ordered commands and asynchronous server messages."""

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from typing import Protocol, Self

from velis.core.messages import Command, Err, Message, Reply
from velis.errors import ProtocolError
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol
from velis.validation import require_positive

_CLOSED = "connection closed"


@dataclass(frozen=True, slots=True)
class SessionLimits:
    """Bounds on in-flight requests and on messages the caller has not read."""

    max_pending_requests: int = 128
    max_pending_messages: int = 1024
    max_pending_bytes: int = 8 << 20

    def __post_init__(self) -> None:
        require_positive(
            max_pending_requests=self.max_pending_requests,
            max_pending_messages=self.max_pending_messages,
            max_pending_bytes=self.max_pending_bytes,
        )


DEFAULT_SESSION_LIMITS = SessionLimits()


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
        wire: WireProtocol = DEFAULT_PROTOCOL,
        limits: SessionLimits = DEFAULT_SESSION_LIMITS,
    ) -> None:
        self._wire = wire
        self._reader = reader
        self._writer = writer
        self._slots = asyncio.BoundedSemaphore(limits.max_pending_requests)
        self._pending: deque[asyncio.Future[Reply]] = deque()
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
        wire: WireProtocol = DEFAULT_PROTOCOL,
        limits: SessionLimits = DEFAULT_SESSION_LIMITS,
    ) -> Self:
        """Open a TCP connection and start reading replies and server pushes."""
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer, wire=wire, limits=limits)

    async def _receive_messages(self) -> None:
        """Route replies and pushes until closure or a transport/protocol error.

        EOF and OSError close normally; protocol errors preserve their reason.
        Unexpected reader failures remain available to close() after cleanup.
        """
        try:
            async with contextlib.aclosing(
                self._wire.read_messages(self._reader)
            ) as messages:
                async for message in messages:
                    if not isinstance(message, Message):
                        self._resolve_reply(message)
                    elif not self._messages.offer(message):
                        self._begin_close("pending message buffer limit exceeded")
                    if self._close_reason is not None:
                        break
        except ProtocolError as exc:
            self._begin_close(str(exc))
        except asyncio.IncompleteReadError, OSError:
            pass
        finally:
            self._begin_close()

    def _resolve_reply(self, reply: Reply) -> None:
        """Resolve the oldest request or discard its cancelled future's reply.

        An unsolicited Err closes the session with the server's rejection reason.
        """
        if not self._pending:
            if isinstance(reply, Err):
                self._begin_close(reply.message)
                return
            raise ProtocolError("reply received without a pending request")
        waiter = self._pop_pending()
        if not waiter.done():
            waiter.set_result(reply)

    def _pop_pending(self) -> asyncio.Future[Reply]:
        """Remove the oldest request and return its reserved capacity."""
        waiter = self._pending.popleft()
        self._slots.release()
        return waiter

    def _begin_close(self, reason: str = _CLOSED) -> None:
        """Close and wake callers, preserving queued messages for draining.

        Repeated calls retain the first close reason and do no further work.
        Mark failed futures as retrieved so abandoned callers leave no warnings.
        """
        if self._close_reason is not None:
            return
        self._close_reason = reason
        self._writer.close()
        while self._pending:
            waiter = self._pop_pending()
            if not waiter.done():
                waiter.set_exception(ConnectionError(reason))
                waiter.exception()
        self._messages.close()

    def _check_open(self) -> None:
        if self._close_reason is not None:
            raise ConnectionError(self._close_reason)

    def _submit(self, command: Command) -> asyncio.Future[Reply]:
        """Encode, write and queue one command without yielding to the loop.

        Staying synchronous keeps FIFO order equal to wire order. Once queued,
        the caller's slot belongs to the future: a reply or closure releases it.
        """
        self._check_open()
        encoded = self._wire.framed_commands.encode(command)
        waiter = asyncio.get_running_loop().create_future()
        self._writer.write(encoded)
        self._pending.append(waiter)
        return waiter

    async def request(self, command: Command) -> Reply:
        """Send a command and await the reply that answers it.

        Cancellation abandons the reply without closing the stream. A sent
        request keeps its slot until its reply arrives, which bounds the
        bookkeeping for cancelled requests even if the peer stops responding.
        Capacity is acquired before wire encoding, and a failed submit returns
        it. Cancellation during drain also retains the future's position while
        discarding its eventual result.
        """
        self._check_open()
        await self._slots.acquire()
        try:
            waiter = self._submit(command)
        except OSError:
            self._slots.release()
            await self.close()
            raise
        except BaseException:
            self._slots.release()
            raise
        try:
            await self._writer.drain()
            return await waiter
        except OSError:
            await self.close()
            raise
        finally:
            waiter.cancel()

    async def next_message(self) -> Message:
        """Wait for the next encoded message.

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
