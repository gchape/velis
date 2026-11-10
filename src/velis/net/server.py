"""TCP listener ownership, admission and connection creation."""

import asyncio
import logging
from types import TracebackType
from typing import Self

from velis.core.engine import Engine
from velis.net.connection import Connection
from velis.settings import ServerLimits

log = logging.getLogger(__name__)


class TCPServer:
    """Own a listener and the admission count of active client transports."""

    def __init__(
        self,
        engine: Engine,
        limits: ServerLimits,
    ) -> None:
        self._engine = engine
        self._limits = limits
        self._listener: asyncio.Server | None = None
        self._connection_count = 0

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
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
        self._listener = await asyncio.get_running_loop().create_server(
            self._new_connection, self._limits.host, self._limits.port
        )
        try:
            log.info("listening on %s:%d", self._limits.host, self.port)
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        """Stop accepting, allow a bounded flush, then abort remaining clients.

        Cancellation also aborts clients before propagating, so losing the
        listener reference never leaves its transports without an owner.
        """
        listener = self._listener
        if listener is None:
            return
        self._listener = None
        listener.close()
        listener.close_clients()
        try:
            async with asyncio.timeout(self._limits.close_timeout_seconds):
                await listener.wait_closed()
        except TimeoutError:
            listener.abort_clients()
            await listener.wait_closed()
        except BaseException:
            listener.abort_clients()
            await listener.wait_closed()
            raise

    def _new_connection(self) -> Connection:
        return Connection(
            self._engine,
            self._limits.max_buffer,
            admit=self._admit,
            release=self._release,
        )

    def _admit(self) -> bool:
        if self._connection_count >= self._limits.max_connections:
            return False
        self._connection_count += 1
        return True

    def _release(self) -> None:
        self._connection_count -= 1
