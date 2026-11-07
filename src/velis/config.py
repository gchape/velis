from __future__ import annotations

import logging
import logging.config
import os
from dataclasses import dataclass

_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s:%(lineno)d  %(message)s"


@dataclass(frozen=True, slots=True)
class Server:
    host: str
    port: int

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f"invalid port: {self.port}")

    @classmethod
    def from_env(cls) -> Server:
        return cls(
            host=os.environ.get("VELIS_HOST", "127.0.0.1"),
            port=int(os.environ.get("VELIS_PORT", "9000")),
        )


@dataclass(frozen=True, slots=True)
class Config:
    log_level: str
    server: Server

    def __post_init__(self) -> None:
        if self.log_level not in logging.getLevelNamesMapping():
            raise ValueError(f"invalid log level: {self.log_level!r}")

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            log_level=os.environ.get("VELIS_LOG_LEVEL", "info").upper(),
            server=Server.from_env(),
        )

    def configure_logging(self) -> None:
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
                        "level": self.log_level,
                        "handlers": ["console"],
                        "propagate": False,
                    },
                },
            }
        )
