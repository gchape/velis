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
REPLY_BYTES_PER_TURN = 1 << 18


@dataclass(slots=True)
class _Batch:
    """Completed replies and consumed input, plus any subsequent protocol error.

    ``limited`` means a per-turn limit ended the batch, so more complete
    commands may be buffered even though none was left unhandled by an error.
    """

    replies: list[bytes]
    consumed: int = 0
    limited: bool = False
    error: ProtocolError | None = None


class Connection(asyncio.Protocol):
    """Run ordered commands and batch replies on the engine's event-loop thread.

    Process at most COMMANDS_PER_TURN commands, or until REPLY_BYTES_PER_TURN of
    replies are queued, before yielding, so a client that stops reading holds
    at most one reply budget plus one maximum-size reply in the write buffer.
    Pending work and write backpressure pause reads; transport recovery resumes
    buffered commands.
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
        self._reading = True
        self._write_blocked = False
        self._eof = False
        self._closing = False
        self._peer: object = None
        self._transport: asyncio.Transport | None = None
        self._pump_handle: asyncio.Handle | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = cast(asyncio.Transport, transport)
        self._peer = transport.get_extra_info("peername")
        if not self._admit():
            self._write_now(Err("connection limit exceeded"))
            self._shutdown()
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
        self._eof = True
        if self._pump_handle is None:
            self._pump()
        return True

    def pause_writing(self) -> None:
        self._write_blocked = True
        self._set_reading(False)

    def resume_writing(self) -> None:
        self._write_blocked = False
        if self._pump_handle is None:
            self._pump()

    def _pump(self) -> None:
        """Execute and flush one batch, then apply its next flow-control state."""
        self._pump_handle = None
        transport = self._transport
        if (
            transport is None
            or self._closing
            or transport.is_closing()
            or self._write_blocked
        ):
            return
        try:
            batch = self._execute_batch(transport)
            del self._buf[: batch.consumed]
            if batch.replies:
                transport.write(b"".join(batch.replies))
        except Exception as exc:
            if not isinstance(exc, OSError):
                log.exception("unexpected error serving %s", self._peer)
            self._shutdown(abort=True)
            return
        if batch.error is not None:
            log.warning("protocol error from %s: %s", self._peer, batch.error)
            self._write_now(Err(str(batch.error)))
            self._shutdown()
        elif batch.limited and self._buf:
            self._yield_to_loop()
        elif self._eof:
            self._shutdown()
        elif not self._write_blocked:
            self._set_reading(True)

    def _execute_batch(self, transport: asyncio.Transport) -> _Batch:
        """Run a bounded frame prefix, retaining replies before a protocol error."""
        decode = protocol.COMMANDS.decode
        encode = protocol.encode_message
        execute = self._engine.execute
        replies: list[bytes] = []
        consumed = queued = 0
        limited = False
        error: ProtocolError | None = None
        try:
            for start, end in protocol.iter_frame_spans(self._buf):
                command = decode(self._buf, start, end)
                try:
                    reply: Reply = execute(command, self)
                except CommandError as exc:
                    reply = Err(str(exc))
                data = encode(reply)
                replies.append(data)
                consumed = end
                queued += len(data)
                if len(replies) == COMMANDS_PER_TURN or queued >= REPLY_BYTES_PER_TURN:
                    limited = True
                    break
                if transport.is_closing():
                    break
        except ProtocolError as exc:
            error = exc
        return _Batch(replies, consumed, limited, error)

    def _yield_to_loop(self) -> None:
        self._set_reading(False)
        self._pump_handle = asyncio.get_running_loop().call_soon(self._pump)

    def _set_reading(self, reading: bool) -> None:
        transport = self._transport
        if (
            transport is None
            or self._reading == reading
            or self._eof
            or transport.is_closing()
        ):
            return
        if reading:
            transport.resume_reading()
        else:
            transport.pause_reading()
        self._reading = reading

    def _write_now(self, reply: Reply) -> None:
        transport = self._transport
        if transport is not None and not transport.is_closing():
            with contextlib.suppress(ProtocolError, OSError):
                transport.write(protocol.encode_message(reply))

    def _cancel_pump(self) -> None:
        if self._pump_handle is not None:
            self._pump_handle.cancel()
            self._pump_handle = None

    def _shutdown(self, *, abort: bool = False) -> None:
        self._closing = True
        self._cancel_pump()
        self._buf.clear()
        if (transport := self._transport) is not None:
            if abort:
                transport.abort()
            else:
                transport.close()

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
