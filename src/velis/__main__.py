import asyncio
import logging.config
import signal
from contextlib import ExitStack

from velis import banner
from velis.app import serve
from velis.config import Config

_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s:%(lineno)d  %(message)s"


def configure_logging(level: str) -> None:
    """Configure timestamped console logging at the CLI boundary."""
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


async def _main(config: Config) -> None:
    """Install CLI signal handlers around the independently owned runtime."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    with ExitStack() as handlers:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
            handlers.callback(loop.remove_signal_handler, sig)
        async with serve(config):
            await stop.wait()


def main() -> None:
    """Load configuration, set up logging and start the server."""
    config = Config.from_env()
    configure_logging(config.log_level)
    banner.show()
    asyncio.run(_main(config))


if __name__ == "__main__":
    main()
