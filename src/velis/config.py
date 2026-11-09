"""Validated application configuration and environment overrides."""

import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Self

from velis.core.broker import DEFAULT_MAX_SUBSCRIPTIONS
from velis.core.eviction import EvictionPolicy
from velis.core.store import StoreLimits
from velis.validation import require_positive

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000
DEFAULT_MAX_BUFFER = 8 << 20
DEFAULT_MAX_CONNECTIONS = 1024
DEFAULT_SWEEP_INTERVAL_MS = 100
DEFAULT_LOG_LEVEL = "INFO"


def _from_env[T](
    cls: type[T],
    /,
    *,
    values: Mapping[str, Any] | None = None,
    **spec: tuple[str, Callable[[str], Any]],
) -> T:
    """Build cls using its defaults, supplied nested values and env overrides."""
    kwargs = dict(values) if values is not None else {}
    for attr, (variable, parse) in spec.items():
        if (raw := os.environ.get(variable)) is not None:
            try:
                kwargs[attr] = parse(raw)
            except ValueError as exc:
                raise ValueError(f"{variable}: {exc}") from exc
    return cls(**kwargs)


@dataclass(frozen=True, slots=True)
class ServerConfig:
    """Validated TCP listener, connection and subscriber buffer settings."""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    max_buffer: int = DEFAULT_MAX_BUFFER
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f"invalid port: {self.port}")
        require_positive(
            max_buffer=self.max_buffer,
            max_connections=self.max_connections,
            max_subscriptions=self.max_subscriptions,
        )

    @classmethod
    def from_env(cls) -> Self:
        """Override only the listener settings present in VELIS_* variables."""
        return _from_env(
            cls,
            host=("VELIS_HOST", str),
            port=("VELIS_PORT", int),
            max_buffer=("VELIS_MAX_BUFFER", int),
            max_connections=("VELIS_MAX_CONNECTIONS", int),
            max_subscriptions=("VELIS_MAX_SUBSCRIPTIONS", int),
        )


@dataclass(frozen=True, slots=True)
class Config:
    """Validated logging, TCP server, store and expiry sweep settings."""

    log_level: str = DEFAULT_LOG_LEVEL
    server: ServerConfig = field(default_factory=ServerConfig)
    sweep_interval_ms: int = DEFAULT_SWEEP_INTERVAL_MS
    store: StoreLimits = field(default_factory=StoreLimits)

    def __post_init__(self) -> None:
        object.__setattr__(self, "log_level", self.log_level.upper())
        require_positive(sweep_interval_ms=self.sweep_interval_ms)
        if self.log_level not in logging.getLevelNamesMapping():
            raise ValueError(f"invalid log level: {self.log_level!r}")

    @classmethod
    def from_env(cls) -> Self:
        """Apply environment overrides while retaining dataclass defaults."""
        return _from_env(
            cls,
            values={
                "server": ServerConfig.from_env(),
                "store": _from_env(
                    StoreLimits,
                    max_keys=("VELIS_MAX_KEYS", int),
                    max_bytes=("VELIS_MAX_BYTES", int),
                    eviction_policy=("VELIS_EVICTION_POLICY", EvictionPolicy),
                ),
            },
            log_level=("VELIS_LOG_LEVEL", str),
            sweep_interval_ms=("VELIS_SWEEP_MS", int),
        )
