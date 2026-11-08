import logging
import os
from dataclasses import dataclass, field

from velis.core.broker import DEFAULT_MAX_SUBSCRIPTIONS, validate_subscription_limit
from velis.core.store import (
    DEFAULT_EVICTION_POLICY,
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_KEYS,
    StoreLimits,
)
from velis.core.store import (
    EVICTION_POLICIES as EVICTION_POLICIES,
)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000
DEFAULT_MAX_BUFFER = 8 << 20
DEFAULT_MAX_CONNECTIONS = 1024
DEFAULT_SWEEP_INTERVAL_MS = 100
DEFAULT_LOG_LEVEL = "INFO"

# Preserve the existing configuration name; core owns limits and validation.
Cache = StoreLimits


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

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT

    max_buffer: int = DEFAULT_MAX_BUFFER
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f"invalid port: {self.port}")
        if self.max_buffer <= 0:
            raise ValueError(f"invalid max_buffer: {self.max_buffer}")
        if self.max_connections <= 0:
            raise ValueError(f"invalid max_connections: {self.max_connections}")
        validate_subscription_limit(self.max_subscriptions)

    @classmethod
    def from_env(cls) -> Server:
        """Load listener settings from VELIS_HOST, VELIS_PORT and VELIS_MAX_BUFFER."""
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
    """Validated logging, TCP server and expiry sweep settings."""

    log_level: str = DEFAULT_LOG_LEVEL
    server: Server = field(default_factory=Server)
    sweep_interval_ms: int = DEFAULT_SWEEP_INTERVAL_MS
    cache: StoreLimits = field(default_factory=StoreLimits)

    def __post_init__(self) -> None:
        if self.sweep_interval_ms <= 0:
            raise ValueError(f"invalid sweep interval: {self.sweep_interval_ms}")
        if self.log_level not in logging.getLevelNamesMapping():
            raise ValueError(f"invalid log level: {self.log_level!r}")

    @classmethod
    def from_env(cls) -> Config:
        """Load configuration from VELIS_* environment variables."""
        return cls(
            log_level=os.environ.get("VELIS_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper(),
            server=Server.from_env(),
            sweep_interval_ms=_env_int("VELIS_SWEEP_MS", DEFAULT_SWEEP_INTERVAL_MS),
            cache=StoreLimits(
                max_keys=_env_int("VELIS_MAX_KEYS", DEFAULT_MAX_KEYS),
                max_bytes=_env_int("VELIS_MAX_BYTES", DEFAULT_MAX_BYTES),
                eviction_policy=os.environ.get(
                    "VELIS_EVICTION_POLICY", DEFAULT_EVICTION_POLICY
                ),
            ),
        )
