"""Compose the store, broker and TCP server and own their runtime lifecycle."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from velis.core.broker import DEFAULT_BROKER_LIMITS, Broker, BrokerLimits
from velis.core.engine import Engine
from velis.core.store import DEFAULT_STORE_LIMITS, Store, StoreLimits
from velis.net.server import DEFAULT_SERVER_LIMITS, ServerLimits, TCPServer
from velis.validation import require_positive

log = logging.getLogger(__name__)
DEFAULT_SWEEP_INTERVAL_MS = 100


def build_engine(
    *,
    store_limits: StoreLimits = DEFAULT_STORE_LIMITS,
    broker_limits: BrokerLimits = DEFAULT_BROKER_LIMITS,
) -> Engine:
    """Create an engine with an empty in-memory store and pub-sub broker."""
    return Engine(
        Store(limits=store_limits),
        Broker(limits=broker_limits),
    )


async def _maintain(engine: Engine, interval: float) -> None:
    """Sweep periodically, yielding between due batches; interval is in seconds."""
    while True:
        await asyncio.sleep(interval)
        engine.sweep()
        while engine.has_due_expirations:
            await asyncio.sleep(0)
            engine.sweep()


@asynccontextmanager
async def serve(
    server_limits: ServerLimits = DEFAULT_SERVER_LIMITS,
    *,
    store_limits: StoreLimits = DEFAULT_STORE_LIMITS,
    broker_limits: BrokerLimits = DEFAULT_BROKER_LIMITS,
    sweep_interval_ms: int = DEFAULT_SWEEP_INTERVAL_MS,
    engine: Engine | None = None,
) -> AsyncGenerator[TCPServer]:
    """Own server and maintenance resources across startup, serving and cleanup.

    Supply an engine to reuse application state or an injected clock.
    Store and broker limits apply only when creating the default engine;
    supplied engines keep their existing limits. Nonpositive sweep intervals
    raise ValueError before any runtime resources are opened.
    Signal handling belongs to the CLI; embedding callers control this scope.
    Maintenance failure interrupts the caller and closes the server. TaskGroup
    reports worker or caller failures in an ExceptionGroup after cleanup.
    """
    require_positive(sweep_interval_ms=sweep_interval_ms)
    if engine is None:
        engine = build_engine(store_limits=store_limits, broker_limits=broker_limits)
    async with (
        TCPServer(engine, server_limits) as server,
        asyncio.TaskGroup() as workers,
    ):
        workers.create_task(_maintain(engine, sweep_interval_ms / 1000))
        try:
            yield server
        finally:
            workers.cancel()
            log.info("shutting down")
