"""Composition root: wires config -> engine -> server, owns the lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

from velis.config import Config
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.store import Store
from velis.net.server import TcpServer

log = logging.getLogger(__name__)


def build_engine() -> Engine:
    """Create an engine with an empty in-memory store and pub-sub broker."""
    return Engine(Store(), Broker())


async def _maintain(engine: Engine, interval: float) -> None:
    """Sweep expired keys periodically, with interval measured in seconds."""
    while True:
        await asyncio.sleep(interval)
        try:
            engine.sweep()
        except Exception:
            log.exception("maintenance failed")


async def _main(config: Config) -> None:
    """Serve until SIGINT or SIGTERM, then stop maintenance and networking."""
    engine = build_engine()
    server = TcpServer(engine, config.server)
    await server.start()
    maintenance = asyncio.create_task(
        _maintain(engine, config.sweep_interval_ms / 1000)
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        log.info("shutting down")
        maintenance.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await maintenance
        await server.close()


def run(config: Config) -> None:
    """Run the configured server in a new asyncio event loop."""
    asyncio.run(_main(config))
