import logging
import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    """Read an integer setting, naming the environment variable on failure."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None


@dataclass(frozen=True, slots=True)
class Server:
    """Validated TCP listener and subscriber buffer settings.

    max_buffer caps queued server writes when pushing messages; allow
    room for channel and payload fields plus protocol framing.
    """

    host: str
    port: int

    max_buffer: int = 8 << 20

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f"invalid port: {self.port}")
        if self.max_buffer <= 0:
            raise ValueError(f"invalid max_buffer: {self.max_buffer}")

    @classmethod
    def from_env(cls) -> Server:
        """Load listener settings from VELIS_HOST, VELIS_PORT and VELIS_MAX_BUFFER."""
        return cls(
            host=os.environ.get("VELIS_HOST", "127.0.0.1"),
            port=_env_int("VELIS_PORT", 9000),
            max_buffer=_env_int("VELIS_MAX_BUFFER", 8 << 20),
        )


@dataclass(frozen=True, slots=True)
class Config:
    """Validated logging, TCP server and expiry sweep settings."""

    log_level: str
    server: Server
    sweep_interval_ms: int = 100

    def __post_init__(self) -> None:
        if self.sweep_interval_ms <= 0:
            raise ValueError(f"invalid sweep interval: {self.sweep_interval_ms}")
        if self.log_level not in logging.getLevelNamesMapping():
            raise ValueError(f"invalid log level: {self.log_level!r}")

    @classmethod
    def from_env(cls) -> Config:
        """Load configuration from VELIS_* environment variables."""
        return cls(
            log_level=os.environ.get("VELIS_LOG_LEVEL", "info").upper(),
            server=Server.from_env(),
            sweep_interval_ms=_env_int("VELIS_SWEEP_MS", 100),
        )
