import asyncio
import logging.config
import signal
from contextlib import AbstractAsyncContextManager, ExitStack

from velis.app import serve
from velis.branding import show_banner
from velis.config import Config, normalize_log_level
from velis.net.server import TCPServer

_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s:%(lineno)d  %(message)s"


def configure_logging(level: str) -> None:
    """Configure console logging; reject unknown levels before changing logging."""
    level = normalize_log_level(level)
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
    """Load validated environment settings, configure logging and start."""
    config = Config.from_env()
    configure_logging(config.log_level)
    show_banner()
    asyncio.run(
        _main(
            serve(
                config.server_limits,
                store_limits=config.store_limits,
                max_subscriptions=config.max_subscriptions,
                sweep_interval_ms=config.sweep_interval_ms,
            )
        )
    )


if __name__ == "__main__":
    main()
