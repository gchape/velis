"""Composition root: wires config -> engine -> server, owns the lifecycle."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from velis.config import Config
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.store import Store
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol
from velis.net.server import TCPServer

log = logging.getLogger(__name__)
_DEFAULT_CONFIG = Config()


def build_engine(config: Config = _DEFAULT_CONFIG) -> Engine:
    """Create an engine with an empty in-memory store and pub-sub broker."""
    return Engine(
        Store(limits=config.store),
        Broker(max_subscriptions=config.server.max_subscriptions),
    )


async def _maintain(engine: Engine, interval: float) -> None:
    """Sweep periodically and yield between rounds while ready timers remain.

    The interval is in seconds. Cascades count as maintenance work even when a
    round expires no values.
    """
    while True:
        await asyncio.sleep(interval)
        engine.sweep()
        while engine.has_due_expirations:
            await asyncio.sleep(0)
            engine.sweep()


@asynccontextmanager
async def serve(
    config: Config,
    *,
    engine: Engine | None = None,
    wire: WireProtocol = DEFAULT_PROTOCOL,
) -> AsyncGenerator[TCPServer]:
    """Own server and maintenance resources across startup, serving and cleanup.

    Supply an engine to reuse application state or an injected clock, and
    a wire protocol to compose custom serialization and validation.
    Signal handling belongs to the CLI; embedding callers control this scope.
    Maintenance failure interrupts the caller and closes the server. TaskGroup
    reports worker or caller failures in an ExceptionGroup after cleanup.
    """
    if engine is None:
        engine = build_engine(config)
    async with (
        TCPServer(engine, config.server, wire=wire) as server,
        asyncio.TaskGroup() as workers,
    ):
        maintenance = workers.create_task(
            _maintain(engine, config.sweep_interval_ms / 1000)
        )
        try:
            yield server
        finally:
            maintenance.cancel()
            log.info("shutting down")
