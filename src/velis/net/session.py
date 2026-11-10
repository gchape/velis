"""Transport sessions for ordered commands and asynchronous server messages."""

import asyncio
import contextlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol, Self

from velis.core.messages import Command, Err, Message, Reply
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.defaults import (
    DEFAULT_CLOSE_TIMEOUT_SECONDS,
    DEFAULT_HOST,
    DEFAULT_PORT,
)
from velis.net.pending import PendingReplies
from velis.validation import require_positive, require_positive_int

DEFAULT_MAX_PENDING_REQUESTS = 64
DEFAULT_MAX_PENDING_MESSAGES = 1024
DEFAULT_MAX_PENDING_BYTES = 8 << 20
DEFAULT_MAX_BATCH_BYTES = 256 << 10


@dataclass(frozen=True, slots=True)
class SessionLimits:
    """Bounds on in-flight requests, batch writes and unread client messages.

    The batch byte target includes framing. One valid command may exceed it
    and is then written alone, so reducing the target never rejects a command.
    """

    max_pending_requests: int = DEFAULT_MAX_PENDING_REQUESTS
    max_pending_messages: int = DEFAULT_MAX_PENDING_MESSAGES
    max_pending_bytes: int = DEFAULT_MAX_PENDING_BYTES
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES
    close_timeout_seconds: float = DEFAULT_CLOSE_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        require_positive_int(
            max_pending_requests=self.max_pending_requests,
            max_pending_messages=self.max_pending_messages,
            max_pending_bytes=self.max_pending_bytes,
            max_batch_bytes=self.max_batch_bytes,
        )
        require_positive(close_timeout_seconds=self.close_timeout_seconds)


DEFAULT_SESSION_LIMITS = SessionLimits()

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

    async def batch(self, commands: Iterable[Command]) -> list[Reply]:
        """Exchange ordered commands for replies, retaining server error replies."""
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
        self._pending = PendingReplies(
            limits.max_pending_requests, limits.max_batch_bytes
        )
        self._close_timeout_seconds = limits.close_timeout_seconds
        self._messages = _MessageBuffer(
            limits.max_pending_messages, limits.max_pending_bytes
        )
        self._close_reason: str | None = None
        self._failure: Exception | None = None
        self._reader_task = asyncio.create_task(self._receive_messages())

    @classmethod
    async def connect(
        cls,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        *,
        limits: SessionLimits = DEFAULT_SESSION_LIMITS,
    ) -> Self:
        """Open a TCP connection and start reading replies and server pushes."""
        reader, writer = await asyncio.open_connection(host, port)
        try:
            return cls(reader, writer, limits=limits)
        except BaseException:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            raise

    async def _receive_messages(self) -> None:
        """Route replies and pushes until closure or a transport/protocol error.

        EOF and OSError close normally; protocol errors preserve their reason.
        An unexpected failure becomes the close reason seen by waiting callers
        and is raised once by close() after cleanup, so no task exception is
        ever left unretrieved.
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
        except Exception as exc:
            self._failure = exc
            self._begin_close(f"reader failed: {exc!r}")
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
        if not self._pending.resolve(reply):
            if isinstance(reply, Err):
                self._begin_close(reply.message)
                return
            raise ProtocolError("reply received without a pending request") from None

    def _begin_close(self, reason: str = _CLOSED) -> None:
        """Close and wake callers, preserving queued messages for draining.

        Repeated calls retain the first close reason and do no further work.
        Mark failed futures as retrieved so abandoned callers leave no warnings.
        """
        if self._close_reason is not None:
            return
        self._close_reason = reason
        self._writer.close()
        self._pending.close(reason)
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
        return (await self.batch((command,)))[0]

    async def batch(self, commands: Iterable[Command]) -> list[Reply]:
        """Validate all commands, then write and collect bounded request windows.

        Each window uses one write and drain, with at most max_pending_requests
        unanswered requests across all callers and a max_batch_bytes write target.
        A command larger than that target is written alone. Waiter registration
        and writing never yield, keeping reply order aligned across callers.
        Other calls may run between windows. Err replies keep their result positions.

        Encoding retains the entire batch in memory before sending. Cancellation
        discards replies for the sent window and sends no further windows, while
        keeping sent slots occupied until their replies arrive or the stream closes.
        """
        frames = tuple(protocol.encode_command(command) for command in commands)
        replies: list[Reply] = []
        waiters: list[asyncio.Future[Reply]] = []
        offset = 0
        try:
            while offset < len(frames):
                waiters = await self._write_batch(frames, offset)
                offset += len(waiters)
                for waiter in waiters:
                    replies.append(await waiter)
            return replies
        except asyncio.QueueShutDown as exc:
            raise ConnectionError(self._close_reason) from exc
        except Exception:
            await self.close()
            raise
        finally:
            for waiter in waiters:
                waiter.cancel()

    async def _write_batch(
        self, frames: tuple[bytes, ...], offset: int
    ) -> list[asyncio.Future[Reply]]:
        """Wait for capacity, then register and write one uninterrupted window.

        After the first slot is acquired, there must be no await until write:
        another caller's bytes must never overtake registered reply waiters.
        StreamWriter.drain supports concurrent waiters once the bytes are sent.
        """
        waiters = await self._pending.reserve(frames, offset)
        try:
            self._writer.write(b"".join(frames[offset : offset + len(waiters)]))
            await self._writer.drain()
            return waiters
        except BaseException:
            for waiter in waiters:
                waiter.cancel()
            raise

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
        unexpected reader failure is raised by the first close() to finish
        cleanup, then not again. Expected OSError failures while closing the
        transport are suppressed. A stalled flush is aborted after the close
        timeout; cancelling close also aborts before propagating cancellation.
        """
        self._begin_close()
        self._reader_task.cancel()
        closed = asyncio.create_task(self._writer.wait_closed())
        try:
            async with asyncio.timeout(self._close_timeout_seconds):
                # wait() preserves StreamWriter's close waiter if this scope
                # times out or is cancelled; cleanup still needs to await it.
                await asyncio.wait((self._reader_task, closed))
        except TimeoutError:
            pass
        finally:
            if not closed.done():
                self._writer.transport.abort()
            with contextlib.suppress(OSError):
                await closed
        failure, self._failure = self._failure, None
        if failure is not None:
            raise failure
