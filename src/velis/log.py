from __future__ import annotations

import logging.config
from pathlib import Path

_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


def _file_handler(log_file: Path) -> dict[str, object]:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    return {
        "class": "logging.handlers.RotatingFileHandler",
        "formatter": "default",
        "filename": str(log_file),
        "maxBytes": 5 * 1024 * 1024,
        "backupCount": 5,
        "encoding": "utf-8",
        "delay": True,
    }


def _console_handler() -> dict[str, object]:
    return {"class": "logging.StreamHandler", "formatter": "default"}


def configure(level: str, log_file: Path, production: bool) -> None:
    name = "file" if production else "console"
    handler = _file_handler(log_file) if production else _console_handler()

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"default": {"format": _FORMAT}},
            "handlers": {name: handler},
            "loggers": {
                "velis": {"level": level, "handlers": [name], "propagate": False},
            },
        }
    )
