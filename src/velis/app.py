"""Compose the store, broker and TCP server and own their runtime lifecycle."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from velis.config import (
    DEFAULT_MAX_SUBSCRIPTIONS,
    DEFAULT_SERVER_LIMITS,
    DEFAULT_STORE_LIMITS,
    DEFAULT_SWEEP_INTERVAL_MS,
    ServerLimits,
    StoreLimits,
)
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.store import Store
from velis.net.server import TCPServer
from velis.validation import require_positive

log = logging.getLogger(__name__)


def build_engine(
    *,
    store_limits: StoreLimits = DEFAULT_STORE_LIMITS,
    max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS,
) -> Engine:
    """Create an engine with an empty in-memory store and pub-sub broker."""
    return Engine(
        Store(limits=store_limits),
        Broker(max_subscriptions=max_subscriptions),
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
    max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS,
    sweep_interval_ms: int = DEFAULT_SWEEP_INTERVAL_MS,
    engine: Engine | None = None,
) -> AsyncGenerator[TCPServer]:
    """Own server and maintenance resources across startup, serving and cleanup.

    Supply an engine to reuse application state or an injected clock.
    Store limits and max_subscriptions apply only when creating the default engine;
    supplied engines keep their existing limits. Nonpositive sweep intervals
    raise ValueError before any runtime resources are opened.
    Signal handling belongs to the CLI; embedding callers control this scope.
    Maintenance failure interrupts the caller and closes the server. TaskGroup
    reports worker or caller failures in an ExceptionGroup after cleanup.
    """
    require_positive(sweep_interval_ms=sweep_interval_ms)
    if engine is None:
        engine = build_engine(
            store_limits=store_limits, max_subscriptions=max_subscriptions
        )
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
