"""Server-side buffered framing, ordered execution and batched replies."""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import cast

from velis.core.engine import Engine
from velis.core.messages import Err, Message, Reply
from velis.errors import CommandError, ProtocolError
from velis.net import protocol

log = logging.getLogger(__name__)
COMMANDS_PER_TURN = 64


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
        self._reading = True
        self._write_blocked = False
        self._closing = False
        self._eof = False
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
        """Execute a bounded batch and write its replies in one transport call."""
        self._pump_handle = None
        transport = self._transport
        if (
            transport is None
            or self._closing
            or transport.is_closing()
            or self._write_blocked
        ):
            return
        decode = protocol.COMMANDS.decode
        encode = protocol.encode_message
        execute = self._engine.execute
        out: list[bytes] = []
        consumed = handled = 0
        error: ProtocolError | None = None
        try:
            for body, end in protocol.iter_frames(self._buf):
                command = decode(body)
                try:
                    reply: Reply = execute(command, self)
                except CommandError as exc:
                    reply = Err(str(exc))
                out.append(encode(reply))
                consumed = end
                handled += 1
                if handled == COMMANDS_PER_TURN or transport.is_closing():
                    break
        except ProtocolError as exc:
            error = exc
        except OSError:
            self._close(abort=True)
            return
        except Exception:
            log.exception("unexpected error serving %s", self._peer)
            self._close(abort=True)
            return
        del self._buf[:consumed]
        try:
            if out:
                transport.write(b"".join(out))
        except OSError:
            self._close(abort=True)
            return
        except Exception:
            log.exception("unexpected write failure serving %s", self._peer)
            self._close(abort=True)
            return
        if error is not None:
            log.warning("protocol error from %s: %s", self._peer, error)
            self._write_now(Err(str(error)))
            self._close()
        elif handled == COMMANDS_PER_TURN and self._buf:
            self._set_reading(False)
            self._pump_handle = asyncio.get_running_loop().call_soon(self._pump)
        elif self._eof:
            self._close()
        elif not self._write_blocked:
            self._set_reading(True)

    def _set_reading(self, on: bool) -> None:
        transport = self._transport
        if (
            transport is not None
            and on != self._reading
            and not transport.is_closing()
            and not self._eof
        ):
            (transport.resume_reading if on else transport.pause_reading)()
            self._reading = on

    def _write_now(self, reply: Reply) -> None:
        transport = self._transport
        if transport is not None and not transport.is_closing():
            with contextlib.suppress(ProtocolError, OSError):
                transport.write(protocol.encode_message(reply))

    def _cancel_pump(self) -> None:
        if self._pump_handle is not None:
            self._pump_handle.cancel()
            self._pump_handle = None

    def _close(self, *, abort: bool = False) -> None:
        self._closing = True
        self._cancel_pump()
        self._buf.clear()
        if (transport := self._transport) is not None:
            (transport.abort if abort else transport.close)()

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
        except (ProtocolError, OSError) as exc:
            if isinstance(exc, ProtocolError):
                log.warning(
                    "dropping subscriber: cannot encode push on %r: %s",
                    message.channel,
                    exc,
                )
            transport.abort()
            return False
        if (
            transport.is_closing()
            or transport.get_write_buffer_size() > self._max_buffer
        ):
            transport.abort()
            return False
        return True
