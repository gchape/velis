from __future__ import annotations

import logging.config
from pathlib import Path


def configure(
    level: str,
    *,
    production: bool,
    log_file: Path | str,
) -> None:
    handlers: dict[str, logging.config._HandlerConfiguration]

    if production:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers = {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "default",
                "filename": str(path),
                "maxBytes": 5 * 1024 * 1024,
                "backupCount": 5,
                "encoding": "utf-8",
                "delay": True,
            },
        }
    else:
        handlers = {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
            },
        }

    config: logging.config._DictConfigArgs = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
            },
        },
        "handlers": handlers,
        "loggers": {
            "velis": {
                "level": level,
                "handlers": list(handlers),
                "propagate": False,
            },
        },
    }
    logging.config.dictConfig(config)
