import asyncio
import contextlib
import logging

from velis.config import ServerConfig
from velis.core.engine import Engine
from velis.core.messages import Err
from velis.errors import CommandError, ProtocolError
from velis.net.connection import Connection, share_message_encodings
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol

log = logging.getLogger(__name__)
COMMANDS_PER_TURN = 64


class TCPServer:
    """Serve commands over TCP within a configured connection limit."""

    def __init__(
        self,
        engine: Engine,
        config: ServerConfig,
        *,
        wire: WireProtocol = DEFAULT_PROTOCOL,
    ) -> None:
        self._wire = wire
        self._engine = engine
        self._config = config
        self._listener: asyncio.Server | None = None
        self._connection_count = 0

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
        self._listener.close_clients()
        await self._listener.wait_closed()
        self._listener = None

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Serve a client and clean up its subscriptions on disconnect.

        Send a protocol error before closing a malformed stream when possible.
        Cleanup still runs if the same codec or transport rejects the error reply.
        """
        peer = writer.get_extra_info("peername")
        connection = Connection(
            reader, writer, self._config.max_buffer, wire=self._wire
        )
        if self._connection_count >= self._config.max_connections:
            await self._reject(connection)
            return
        self._connection_count += 1
        log.info("connected: %s", peer)
        try:
            await self._serve_commands(connection)
        except asyncio.IncompleteReadError, ConnectionError:
            pass
        except ProtocolError as exc:
            log.warning("protocol error from %s: %s", peer, exc)
            with contextlib.suppress(ProtocolError, ConnectionError):
                connection.write_reply(Err(str(exc)))
        except Exception:
            log.exception("unexpected error serving %s", peer)
        finally:
            self._connection_count -= 1
            self._engine.disconnect(connection)
            connection.close()
            await connection.wait_closed()
            log.info("disconnected: %s", peer)

    async def _reject(self, connection: Connection) -> None:
        """Best-effort admission error followed by connection cleanup."""
        try:
            with contextlib.suppress(ProtocolError, ConnectionError):
                connection.write_reply(Err("connection limit exceeded"))
        finally:
            connection.close()
            await connection.wait_closed()

    async def _serve_commands(self, connection: Connection) -> None:
        """Execute in order, yielding after each bounded batch of ready commands."""
        processed = 0
        async for command in connection.read_commands():
            try:
                with share_message_encodings():
                    reply = self._engine.execute(command, connection)
            except CommandError as exc:
                reply = Err(str(exc))
            connection.write_reply(reply)
            await connection.drain()
            processed += 1
            if processed == COMMANDS_PER_TURN:
                processed = 0
                await asyncio.sleep(0)
