import asyncio
import logging.config
import os
import signal
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, ExitStack

from velis.app import DEFAULT_SWEEP_INTERVAL_MS, serve
from velis.core.broker import DEFAULT_BROKER_LIMITS, BrokerLimits
from velis.core.eviction import EvictionPolicy
from velis.core.store import DEFAULT_STORE_LIMITS, StoreLimits
from velis.net.server import DEFAULT_SERVER_LIMITS, ServerLimits, TCPServer

_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s:%(lineno)d  %(message)s"


def _env[T](variable: str, default: T, parse: Callable[[str], T]) -> T:
    """Parse a present environment variable, retaining its name on parse errors.

    Only an absent variable uses the default; empty values are parsed normally.
    Component limit objects validate the parsed values when constructed.
    """
    if (raw := os.environ.get(variable)) is None:
        return default
    try:
        return parse(raw)
    except ValueError as exc:
        raise ValueError(f"{variable}: {exc}") from exc


def configure_logging(level: str) -> None:
    """Configure console logging; reject unknown levels before changing logging."""
    level = level.upper()
    if level not in logging.getLevelNamesMapping():
        raise ValueError(f"invalid log level: {level!r}")
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"default": {"format": _FORMAT}},
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                },
            },
            "loggers": {
                "velis": {
                    "level": level,
                    "handlers": ["console"],
                    "propagate": False,
                },
            },
        }
    )


async def _main(runtime: AbstractAsyncContextManager[TCPServer]) -> None:
    """Install CLI signal handlers around the independently owned runtime."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    with ExitStack() as handlers:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
            handlers.callback(loop.remove_signal_handler, sig)
        async with runtime:
            await stop.wait()


def main() -> None:
    """Load VELIS_* settings into their owning limits and start the runtime."""
    server_limits = ServerLimits(
        host=_env("VELIS_HOST", DEFAULT_SERVER_LIMITS.host, str),
        port=_env("VELIS_PORT", DEFAULT_SERVER_LIMITS.port, int),
        max_buffer=_env("VELIS_MAX_BUFFER", DEFAULT_SERVER_LIMITS.max_buffer, int),
        max_connections=_env(
            "VELIS_MAX_CONNECTIONS", DEFAULT_SERVER_LIMITS.max_connections, int
        ),
    )
    store_limits = StoreLimits(
        max_keys=_env("VELIS_MAX_KEYS", DEFAULT_STORE_LIMITS.max_keys, int),
        max_bytes=_env("VELIS_MAX_BYTES", DEFAULT_STORE_LIMITS.max_bytes, int),
        eviction_policy=_env(
            "VELIS_EVICTION_POLICY",
            DEFAULT_STORE_LIMITS.eviction_policy,
            EvictionPolicy,
        ),
    )
    broker_limits = BrokerLimits(
        max_subscriptions=_env(
            "VELIS_MAX_SUBSCRIPTIONS", DEFAULT_BROKER_LIMITS.max_subscriptions, int
        )
    )
    sweep_interval_ms = _env("VELIS_SWEEP_MS", DEFAULT_SWEEP_INTERVAL_MS, int)
    configure_logging(os.environ.get("VELIS_LOG_LEVEL", "INFO"))
    asyncio.run(
        _main(
            serve(
                server_limits,
                store_limits=store_limits,
                broker_limits=broker_limits,
                sweep_interval_ms=sweep_interval_ms,
            )
        )
    )


if __name__ == "__main__":
    main()
