"""Composition root: wires config -> engine -> server, owns the lifecycle."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager

from velis.config import Config
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.store import Store
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol
from velis.net.server import TcpServer

log = logging.getLogger(__name__)


def build_engine(config: Config | None = None) -> Engine:
    """Create an engine with an empty in-memory store and pub-sub broker."""
    if config is None:
        return Engine(Store(), Broker())
    return Engine(
        Store(max_keys=config.cache.max_keys, max_bytes=config.cache.max_bytes),
        Broker(max_subscriptions=config.server.max_subscriptions),
    )


async def _maintain(engine: Engine, interval: float) -> None:
    """Sweep expired keys periodically, with interval measured in seconds."""
    while True:
        await asyncio.sleep(interval)
        engine.sweep()


@asynccontextmanager
async def serve(
    config: Config,
    *,
    engine: Engine | None = None,
    wire: WireProtocol = DEFAULT_PROTOCOL,
) -> AsyncGenerator[TcpServer]:
    """Own server and maintenance resources across startup, serving and cleanup.

    Supply an engine to reuse application state or an injected clock, and
    a wire protocol to compose custom serialization and validation.
    Signal handling belongs to the CLI; embedding callers control this scope.
    Maintenance failure interrupts the caller and closes the server. TaskGroup
    reports worker or caller failures in an ExceptionGroup after cleanup.
    """
    if engine is None:
        engine = build_engine(config)
    server = TcpServer(engine, config.server, wire=wire)
    async with AsyncExitStack() as resources:
        resources.push_async_callback(server.close)
        await server.start()
        async with asyncio.TaskGroup() as workers:
            maintenance = workers.create_task(
                _maintain(engine, config.sweep_interval_ms / 1000)
            )
            try:
                yield server
            finally:
                maintenance.cancel()
                log.info("shutting down")
