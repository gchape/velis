from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    log_level: str = "INFO"
    production: bool = False
    log_file: Path = Path("/var/log/velis/velis.log")
    host: str = "127.0.0.1"
    port: int = 9000

    def __post_init__(self) -> None:
        if self.log_level not in logging.getLevelNamesMapping():
            raise ValueError(f"invalid log level: {self.log_level!r}")
        if not 0 <= self.port <= 65535:
            raise ValueError(f"invalid port: {self.port}")

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        defaults = cls()
        return cls(
            log_level=env.get("VELIS_LOG_LEVEL", defaults.log_level).upper(),
            production=env.get("VELIS_ENV", "").lower() == "production",
            log_file=Path(env.get("VELIS_LOG_FILE", str(defaults.log_file))),
            host=env.get("VELIS_HOST", defaults.host),
            port=int(env.get("VELIS_PORT", str(defaults.port))),
        )
