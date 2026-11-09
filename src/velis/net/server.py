"""TCP listener ownership, admission and command-error translation."""

import asyncio
import contextlib
import logging
from collections.abc import Generator
from dataclasses import dataclass
from typing import Self

from velis.core.engine import Engine
from velis.core.messages import Err
from velis.errors import CommandError, ProtocolError
from velis.net.connection import Connection
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol
from velis.validation import require_positive

log = logging.getLogger(__name__)
COMMANDS_PER_TURN = 64
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000
DEFAULT_MAX_BUFFER = 8 << 20
DEFAULT_MAX_CONNECTIONS = 1024


@dataclass(frozen=True, slots=True)
class ServerLimits:
    """Validated TCP listener, connection and subscriber buffer settings."""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    max_buffer: int = DEFAULT_MAX_BUFFER
    max_connections: int = DEFAULT_MAX_CONNECTIONS

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f"invalid port: {self.port}")
        require_positive(
            max_buffer=self.max_buffer, max_connections=self.max_connections
        )


DEFAULT_SERVER_LIMITS = ServerLimits()


class TCPServer:
    """Own a listener and active client streams within a connection limit."""

    def __init__(
        self,
        engine: Engine,
        limits: ServerLimits,
        *,
        wire: WireProtocol = DEFAULT_PROTOCOL,
    ) -> None:
        self._wire = wire
        self._engine = engine
        self._limits = limits
        self._listener: asyncio.Server | None = None
        self._connection_count = 0

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, exc_type: object, *_: object) -> None:
        await self.close()

    @property
    def port(self) -> int:
        """Return the bound port; raise RuntimeError before start or after close."""
        if self._listener is None:
            raise RuntimeError("server not started")
        return self._listener.sockets[0].getsockname()[1]

    async def start(self) -> None:
        """Start accepting connections; an active listener cannot be restarted."""
        if self._listener is not None:
            raise RuntimeError("server already started")
        try:
            self._listener = await asyncio.start_server(
                self._handle_connection, self._limits.host, self._limits.port
            )
            log.info("listening on %s:%d", self._limits.host, self.port)
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        """Stop accepting clients and close their streams; safe to repeat."""
        listener = self._listener
        if listener is None:
            return
        self._listener = None
        listener.close()
        listener.close_clients()
        await listener.wait_closed()

    @contextlib.contextmanager
    def _admitted(self, connection: Connection, peer: object) -> Generator[None]:
        """Own admission counting and broker cleanup for an accepted client."""
        self._connection_count += 1
        try:
            log.info("connected: %s", peer)
            yield
        finally:
            self._connection_count -= 1
            self._engine.disconnect(connection)
            log.info("disconnected: %s", peer)

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        async with Connection(
            reader, writer, self._limits.max_buffer, wire=self._wire
        ) as connection:
            if self._connection_count >= self._limits.max_connections:
                with contextlib.suppress(ProtocolError, OSError):
                    await connection.write_reply(Err("connection limit exceeded"))
                return
            peer = writer.get_extra_info("peername")
            with self._admitted(connection, peer):
                await self._serve_guarded(connection, peer)

    async def _serve_guarded(self, connection: Connection, peer: object) -> None:
        """Translate expected command/transport errors and log unexpected failures."""
        try:
            await self._serve_commands(connection)
        except asyncio.IncompleteReadError, OSError:
            pass
        except ProtocolError as exc:
            log.warning("protocol error from %s: %s", peer, exc)
            with contextlib.suppress(ProtocolError, OSError):
                await connection.write_reply(Err(str(exc)))
        except Exception:
            log.exception("unexpected error serving %s", peer)

    async def _serve_commands(self, connection: Connection) -> None:
        """Execute in order and yield between bounded batches of ready commands."""
        processed = 0
        async with contextlib.aclosing(connection.read_commands()) as commands:
            async for command in commands:
                try:
                    reply = self._engine.execute(command, connection)
                except CommandError as exc:
                    reply = Err(str(exc))
                await connection.write_reply(reply)
                processed += 1
                if processed % COMMANDS_PER_TURN == 0:
                    await asyncio.sleep(0)
