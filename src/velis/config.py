import logging
import os
from dataclasses import dataclass, field

from velis.core.broker import DEFAULT_MAX_SUBSCRIPTIONS
from velis.core.store import (
    DEFAULT_EVICTION_POLICY,
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_KEYS,
    EvictionPolicy,
    StoreLimits,
)
from velis.validation import require_positive

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000
DEFAULT_MAX_BUFFER = 8 << 20
DEFAULT_MAX_CONNECTIONS = 1024
DEFAULT_SWEEP_INTERVAL_MS = 100
DEFAULT_LOG_LEVEL = "INFO"


def _env_int(name: str, default: int) -> int:
    """Read an integer setting, naming the environment variable on failure."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_policy(name: str, default: EvictionPolicy) -> EvictionPolicy:
    """Read an eviction policy, naming the environment variable on failure."""
    raw = os.environ.get(name, default)
    try:
        return EvictionPolicy(raw)
    except ValueError as exc:
        choices = ", ".join(EvictionPolicy)
        raise ValueError(f"{name} must be one of {choices}, got {raw!r}") from exc


@dataclass(frozen=True, slots=True)
class ServerConfig:
    """Validated TCP listener and subscriber buffer settings.

    max_buffer caps queued server writes when pushing messages; allow
    room for channel and payload fields plus protocol framing.
    """

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT

    max_buffer: int = DEFAULT_MAX_BUFFER
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f"invalid port: {self.port}")
        require_positive("max_buffer", self.max_buffer)
        require_positive("max_connections", self.max_connections)
        require_positive("max_subscriptions", self.max_subscriptions)

    @classmethod
    def from_env(cls) -> ServerConfig:
        """Load listener settings from VELIS_* environment variables."""
        return cls(
            host=os.environ.get("VELIS_HOST", DEFAULT_HOST),
            port=_env_int("VELIS_PORT", DEFAULT_PORT),
            max_buffer=_env_int("VELIS_MAX_BUFFER", DEFAULT_MAX_BUFFER),
            max_connections=_env_int("VELIS_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS),
            max_subscriptions=_env_int(
                "VELIS_MAX_SUBSCRIPTIONS", DEFAULT_MAX_SUBSCRIPTIONS
            ),
        )


@dataclass(frozen=True, slots=True)
class Config:
    """Validated logging, TCP server, store and expiry sweep settings."""

    log_level: str = DEFAULT_LOG_LEVEL
    server: ServerConfig = field(default_factory=ServerConfig)
    sweep_interval_ms: int = DEFAULT_SWEEP_INTERVAL_MS
    store: StoreLimits = field(default_factory=StoreLimits)

    def __post_init__(self) -> None:
        require_positive("sweep_interval_ms", self.sweep_interval_ms)
        if self.log_level not in logging.getLevelNamesMapping():
            raise ValueError(f"invalid log level: {self.log_level!r}")

    @classmethod
    def from_env(cls) -> Config:
        """Load configuration from VELIS_* environment variables."""
        return cls(
            log_level=os.environ.get("VELIS_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper(),
            server=ServerConfig.from_env(),
            sweep_interval_ms=_env_int("VELIS_SWEEP_MS", DEFAULT_SWEEP_INTERVAL_MS),
            store=StoreLimits(
                max_keys=_env_int("VELIS_MAX_KEYS", DEFAULT_MAX_KEYS),
                max_bytes=_env_int("VELIS_MAX_BYTES", DEFAULT_MAX_BYTES),
                eviction_policy=_env_policy(
                    "VELIS_EVICTION_POLICY", DEFAULT_EVICTION_POLICY
                ),
            ),
        )
