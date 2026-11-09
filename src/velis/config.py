"""Validated application settings, defaults and VELIS_* environment loading."""

import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Self

from velis.core.eviction import EvictionPolicy
from velis.validation import require_positive

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000
DEFAULT_MAX_BUFFER = 8 << 20
DEFAULT_MAX_CONNECTIONS = 1024
DEFAULT_MAX_KEYS = 100_000
DEFAULT_MAX_BYTES = 64 << 20
DEFAULT_EVICTION_POLICY = EvictionPolicy.S3_FIFO
DEFAULT_MAX_SUBSCRIPTIONS = 1024
DEFAULT_SWEEP_INTERVAL_MS = 100
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_MAX_PENDING_REQUESTS = 128
DEFAULT_MAX_PENDING_MESSAGES = 1024
DEFAULT_MAX_PENDING_BYTES = 8 << 20


@dataclass(frozen=True, slots=True)
class ServerLimits:
    """TCP listener, connection and subscriber buffer settings."""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    max_buffer: int = DEFAULT_MAX_BUFFER
    max_connections: int = DEFAULT_MAX_CONNECTIONS

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f"invalid port: {self.port}")
        require_positive(
            max_buffer=self.max_buffer, max_connections=self.max_connections
        )


@dataclass(frozen=True, slots=True)
class StoreLimits:
    """Bound retained keys and UTF-8 key plus value bytes.

    Policy strings are normalized to EvictionPolicy during construction.
    The byte budget excludes Python objects and policy/expiry metadata.
    """

    max_keys: int = DEFAULT_MAX_KEYS
    max_bytes: int = DEFAULT_MAX_BYTES
    eviction_policy: EvictionPolicy = DEFAULT_EVICTION_POLICY

    def __post_init__(self) -> None:
        require_positive(max_keys=self.max_keys, max_bytes=self.max_bytes)
        object.__setattr__(
            self, "eviction_policy", EvictionPolicy(self.eviction_policy)
        )


@dataclass(frozen=True, slots=True)
class SessionLimits:
    """Bounds on in-flight requests and unread client messages."""

    max_pending_requests: int = DEFAULT_MAX_PENDING_REQUESTS
    max_pending_messages: int = DEFAULT_MAX_PENDING_MESSAGES
    max_pending_bytes: int = DEFAULT_MAX_PENDING_BYTES

    def __post_init__(self) -> None:
        require_positive(
            max_pending_requests=self.max_pending_requests,
            max_pending_messages=self.max_pending_messages,
            max_pending_bytes=self.max_pending_bytes,
        )


DEFAULT_SERVER_LIMITS = ServerLimits()
DEFAULT_STORE_LIMITS = StoreLimits()
DEFAULT_SESSION_LIMITS = SessionLimits()


def normalize_log_level(level: str) -> str:
    """Accept logging's named levels case-insensitively."""
    level = level.upper()
    if level not in logging.getLevelNamesMapping():
        raise ValueError(f"invalid log level: {level!r}")
    return level


def _env[T](
    environ: Mapping[str, str], variable: str, default: T, parse: Callable[[str], T]
) -> T:
    """Only missing variables use defaults; retain names on parse errors."""
    if (raw := environ.get(variable)) is None:
        return default
    try:
        return parse(raw)
    except ValueError as exc:
        raise ValueError(f"{variable}: {exc}") from exc


@dataclass(frozen=True, slots=True)
class Config:
    """One immutable snapshot of server, store, broker and client settings.

    Construction uses explicit values and defaults. Call from_env() to read
    the current process environment, or pass a mapping for an isolated load.
    """

    server_limits: ServerLimits = DEFAULT_SERVER_LIMITS
    store_limits: StoreLimits = DEFAULT_STORE_LIMITS
    session_limits: SessionLimits = DEFAULT_SESSION_LIMITS
    max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS
    sweep_interval_ms: int = DEFAULT_SWEEP_INTERVAL_MS
    log_level: str = DEFAULT_LOG_LEVEL

    def __post_init__(self) -> None:
        require_positive(
            max_subscriptions=self.max_subscriptions,
            sweep_interval_ms=self.sweep_interval_ms,
        )
        object.__setattr__(self, "log_level", normalize_log_level(self.log_level))

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Self:
        """Read VELIS_* settings and validate everything before startup."""
        source = os.environ if environ is None else environ
        return cls(
            server_limits=ServerLimits(
                host=_env(source, "VELIS_HOST", DEFAULT_HOST, str),
                port=_env(source, "VELIS_PORT", DEFAULT_PORT, int),
                max_buffer=_env(source, "VELIS_MAX_BUFFER", DEFAULT_MAX_BUFFER, int),
                max_connections=_env(
                    source, "VELIS_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS, int
                ),
            ),
            store_limits=StoreLimits(
                max_keys=_env(source, "VELIS_MAX_KEYS", DEFAULT_MAX_KEYS, int),
                max_bytes=_env(source, "VELIS_MAX_BYTES", DEFAULT_MAX_BYTES, int),
                eviction_policy=_env(
                    source,
                    "VELIS_EVICTION_POLICY",
                    DEFAULT_EVICTION_POLICY,
                    EvictionPolicy,
                ),
            ),
            session_limits=SessionLimits(
                max_pending_requests=_env(
                    source,
                    "VELIS_MAX_PENDING_REQUESTS",
                    DEFAULT_MAX_PENDING_REQUESTS,
                    int,
                ),
                max_pending_messages=_env(
                    source,
                    "VELIS_MAX_PENDING_MESSAGES",
                    DEFAULT_MAX_PENDING_MESSAGES,
                    int,
                ),
                max_pending_bytes=_env(
                    source, "VELIS_MAX_PENDING_BYTES", DEFAULT_MAX_PENDING_BYTES, int
                ),
            ),
            max_subscriptions=_env(
                source, "VELIS_MAX_SUBSCRIPTIONS", DEFAULT_MAX_SUBSCRIPTIONS, int
            ),
            sweep_interval_ms=_env(
                source, "VELIS_SWEEP_MS", DEFAULT_SWEEP_INTERVAL_MS, int
            ),
            log_level=_env(
                source, "VELIS_LOG_LEVEL", DEFAULT_LOG_LEVEL, normalize_log_level
            ),
        )
