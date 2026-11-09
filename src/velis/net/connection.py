"""Server-side buffered framing, ordered execution and batched replies."""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from velis.core.engine import Engine
from velis.core.messages import Err, Message, Reply
from velis.errors import CommandError, ProtocolError
from velis.net import protocol

log = logging.getLogger(__name__)
COMMANDS_PER_TURN = 64


@dataclass(slots=True)
class _Batch:
    """Completed replies and consumed input, plus any subsequent protocol error."""

    replies: list[bytes]
    consumed: int = 0
    handled: int = 0
    error: ProtocolError | None = None


@dataclass(slots=True)
class _FlowControl:
    """Read readiness and write backpressure, separate from connection lifetime.

    Queued batches and blocked writes pause reads. EOF prevents resuming reads,
    while complete buffered commands can still drain once writes resume.
    """

    reading: bool = True
    write_blocked: bool = False
    eof: bool = False


class Connection(asyncio.Protocol):
    """Run ordered commands and batch replies on the engine's event-loop thread.

    Process at most COMMANDS_PER_TURN commands before yielding. Pending work and
    write backpressure pause reads; transport recovery resumes buffered commands.
    EOF drains complete buffered requests before closing. A protocol error flushes
    preceding replies and a best-effort ERR, then discards subsequent commands.
    """

    def __init__(
        self,
        engine: Engine,
        max_buffer: int,
        *,
        admit: Callable[[], bool],
        release: Callable[[], None],
    ) -> None:
        self._engine = engine
        self._max_buffer = max_buffer
        self._admit = admit
        self._release = release
        self._buf = bytearray()
        self._admitted = False
        self._flow = _FlowControl()
        self._closing = False
        self._peer: object = None
        self._transport: asyncio.Transport | None = None
        self._pump_handle: asyncio.Handle | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = cast(asyncio.Transport, transport)
        self._peer = transport.get_extra_info("peername")
        if not self._admit():
            self._write_now(Err("connection limit exceeded"))
            self._close()
            return
        self._admitted = True
        log.info("connected: %s", self._peer)

    def connection_lost(self, exc: Exception | None) -> None:
        self._closing = True
        self._cancel_pump()
        self._buf.clear()
        self._transport = None
        if self._admitted:
            self._admitted = False
            self._release()
            self._engine.disconnect(self)
            log.info("disconnected: %s", self._peer)

    def data_received(self, data: bytes) -> None:
        if self._closing:
            return
        self._buf.extend(data)
        if self._pump_handle is None:
            self._pump()

    def eof_received(self) -> bool:
        """Finish accepted requests after a half-close; discard an incomplete tail."""
        self._flow.eof = True
        if self._pump_handle is None:
            self._pump()
        return True

    def pause_writing(self) -> None:
        self._flow.write_blocked = True
        self._pause_reading()

    def resume_writing(self) -> None:
        self._flow.write_blocked = False
        if self._pump_handle is None:
            self._pump()

    def _pump(self) -> None:
        """Execute and flush one batch, then apply its next flow-control state."""
        self._pump_handle = None
        transport = self._transport
        if transport is None or not self._can_serve(transport):
            return
        try:
            batch = self._execute_batch(transport)
            del self._buf[: batch.consumed]
            if batch.replies:
                transport.write(b"".join(batch.replies))
        except Exception as exc:
            self._abort_after_failure(exc)
            return
        self._schedule_next(batch)

    def _can_serve(self, transport: asyncio.Transport) -> bool:
        return not (self._closing or transport.is_closing() or self._flow.write_blocked)

    def _execute_batch(self, transport: asyncio.Transport) -> _Batch:
        """Run a bounded frame prefix, retaining replies before a protocol error."""
        decode = protocol.COMMANDS.decode
        encode = protocol.encode_message
        execute = self._engine.execute
        replies: list[bytes] = []
        consumed = handled = 0
        error: ProtocolError | None = None
        try:
            for body, end in protocol.iter_frames(self._buf):
                command = decode(body)
                try:
                    reply: Reply = execute(command, self)
                except CommandError as exc:
                    reply = Err(str(exc))
                replies.append(encode(reply))
                consumed = end
                handled += 1
                if handled == COMMANDS_PER_TURN or transport.is_closing():
                    break
        except ProtocolError as exc:
            error = exc
        return _Batch(replies, consumed, handled, error)

    def _abort_after_failure(self, exc: Exception) -> None:
        """Abort failures, logging unexpected errors and keeping OSError quiet."""
        if not isinstance(exc, OSError):
            log.exception("unexpected error serving %s", self._peer)
        self._abort()

    def _schedule_next(self, batch: _Batch) -> None:
        if batch.error is not None:
            self._fail_protocol(batch.error)
        elif batch.handled == COMMANDS_PER_TURN and self._buf:
            self._yield_to_loop()
        elif self._flow.eof:
            self._close()
        elif not self._flow.write_blocked:
            self._resume_reading()

    def _fail_protocol(self, error: ProtocolError) -> None:
        log.warning("protocol error from %s: %s", self._peer, error)
        self._write_now(Err(str(error)))
        self._close()

    def _yield_to_loop(self) -> None:
        self._pause_reading()
        self._pump_handle = asyncio.get_running_loop().call_soon(self._pump)

    def _pause_reading(self) -> None:
        transport = self._transport
        if (
            transport is not None
            and self._flow.reading
            and not transport.is_closing()
            and not self._flow.eof
        ):
            transport.pause_reading()
            self._flow.reading = False

    def _resume_reading(self) -> None:
        transport = self._transport
        if (
            transport is not None
            and not self._flow.reading
            and not transport.is_closing()
            and not self._flow.eof
        ):
            transport.resume_reading()
            self._flow.reading = True

    def _write_now(self, reply: Reply) -> None:
        transport = self._transport
        if transport is not None and not transport.is_closing():
            with contextlib.suppress(ProtocolError, OSError):
                transport.write(protocol.encode_message(reply))

    def _cancel_pump(self) -> None:
        if self._pump_handle is not None:
            self._pump_handle.cancel()
            self._pump_handle = None

    def _begin_close(self) -> asyncio.Transport | None:
        self._closing = True
        self._cancel_pump()
        self._buf.clear()
        return self._transport

    def _close(self) -> None:
        if (transport := self._begin_close()) is not None:
            transport.close()

    def _abort(self) -> None:
        if (transport := self._begin_close()) is not None:
            transport.abort()

    def send(self, message: Message) -> bool:
        """Queue a push and return whether the transport accepted it.

        Closed transports reject without writing. Protocol and OS errors, or a
        write buffer strictly above the limit, abort the transport and return
        False; unexpected exceptions propagate. Acceptance does not confirm that
        the subscriber read the push, and this method never waits for socket I/O.
        """
        transport = self._transport
        if transport is None or self._closing or transport.is_closing():
            return False
        try:
            transport.write(protocol.encode_push(message))
        except ProtocolError as exc:
            log.warning(
                "dropping subscriber: cannot encode push on %r: %s",
                message.channel,
                exc,
            )
            transport.abort()
            return False
        except OSError:
            transport.abort()
            return False
        if (
            transport.is_closing()
            or transport.get_write_buffer_size() > self._max_buffer
        ):
            transport.abort()
            return False
        return True
