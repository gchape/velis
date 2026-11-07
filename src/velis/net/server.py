from __future__ import annotations

import asyncio
import contextlib
import logging

from velis.config import Server as ServerConfig
from velis.core.commands import Err
from velis.core.engine import Engine
from velis.errors import CommandError, ProtocolError
from velis.net import protocol
from velis.net.connection import Connection

log = logging.getLogger(__name__)


class TcpServer:
    """Serve commands over TCP and track active subscriber connections."""

    def __init__(self, engine: Engine, config: ServerConfig) -> None:
        self._engine = engine
        self._config = config
        self._listener: asyncio.Server | None = None
        self._connections: set[Connection] = set()

    @property
    def port(self) -> int:
        """Return the bound port after the server has started."""
        assert self._listener is not None, "server not started"
        return self._listener.sockets[0].getsockname()[1]

    async def start(self) -> None:
        """Start accepting connections on the configured address."""
        self._listener = await asyncio.start_server(
            self._handle_connection, self._config.host, self._config.port
        )
        log.info("listening on %s:%d", self._config.host, self.port)

    async def close(self) -> None:
        """Stop accepting connections and close active client streams."""
        if self._listener is None:
            return
        self._listener.close()
        for connection in list(self._connections):
            connection.close()
        await self._listener.wait_closed()
        self._listener = None

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Serve a client and clean up its subscriptions on disconnect.

        Send a protocol error before closing a malformed stream when possible.
        """
        peer = writer.get_extra_info("peername")
        connection = Connection(reader, writer, self._config.max_buffer)
        self._connections.add(connection)
        log.info("connected: %s", peer)
        try:
            await self._serve_commands(connection)
        except asyncio.IncompleteReadError, ConnectionError:
            pass
        except ProtocolError as exc:
            log.warning("protocol error from %s: %s", peer, exc)
            connection.write(protocol.encode_reply(Err(str(exc))))
        except Exception:
            log.exception("unexpected error serving %s", peer)
        finally:
            self._connections.discard(connection)
            self._engine.disconnect(connection)
            connection.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            log.info("disconnected: %s", peer)

    async def _serve_commands(self, connection: Connection) -> None:
        """Read commands and reply until the connection ends or misbehaves."""
        while True:
            command = await protocol.read_command(connection.reader)
            try:
                reply = self._engine.execute(command, connection)
            except CommandError as exc:
                reply = Err(str(exc))
            connection.write(protocol.encode_reply(reply))
            await connection.drain()
