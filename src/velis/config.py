"""Validated application settings, defaults and VELIS_* environment loading."""

import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Self

from velis.core.broker import DEFAULT_MAX_SUBSCRIPTIONS
from velis.core.eviction import EvictionPolicy
from velis.core.store import (
    DEFAULT_EVICTION_POLICY,
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_KEYS,
    DEFAULT_STORE_LIMITS,
    DEFAULT_SWEEP_INTERVAL_MS,
    StoreLimits,
)
from velis.net.defaults import (
    DEFAULT_CLOSE_TIMEOUT_SECONDS,
    DEFAULT_HOST,
    DEFAULT_PORT,
)
from velis.net.server import (
    DEFAULT_MAX_BUFFER,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_SERVER_LIMITS,
    ServerLimits,
)
from velis.net.session import (
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_PENDING_BYTES,
    DEFAULT_MAX_PENDING_MESSAGES,
    DEFAULT_MAX_PENDING_REQUESTS,
    DEFAULT_SESSION_LIMITS,
    SessionLimits,
)
from velis.validation import require_positive_int

__all__ = [
    "DEFAULT_CLOSE_TIMEOUT_SECONDS",
    "DEFAULT_EVICTION_POLICY",
    "DEFAULT_HOST",
    "DEFAULT_LOG_LEVEL",
    "DEFAULT_MAX_BATCH_BYTES",
    "DEFAULT_MAX_BUFFER",
    "DEFAULT_MAX_BYTES",
    "DEFAULT_MAX_CONNECTIONS",
    "DEFAULT_MAX_KEYS",
    "DEFAULT_MAX_PENDING_BYTES",
    "DEFAULT_MAX_PENDING_MESSAGES",
    "DEFAULT_MAX_PENDING_REQUESTS",
    "DEFAULT_MAX_SUBSCRIPTIONS",
    "DEFAULT_PORT",
    "DEFAULT_SERVER_LIMITS",
    "DEFAULT_SESSION_LIMITS",
    "DEFAULT_STORE_LIMITS",
    "DEFAULT_SWEEP_INTERVAL_MS",
    "Config",
    "ServerLimits",
    "SessionLimits",
    "StoreLimits",
    "normalize_log_level",
]

DEFAULT_LOG_LEVEL = "INFO"


def normalize_log_level(level: str) -> str:
    """Accept logging's named levels case-insensitively."""
    level = level.upper()
    if level not in logging.getLevelNamesMapping():
        raise ValueError(f"invalid log level: {level!r}")
    return level


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
        require_positive_int(
            max_subscriptions=self.max_subscriptions,
            sweep_interval_ms=self.sweep_interval_ms,
        )
        object.__setattr__(self, "log_level", normalize_log_level(self.log_level))

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Self:
        """Read VELIS_* settings and validate everything before startup."""
        env = _Environment(os.environ if environ is None else environ)
        timeout = env.read(
            "CLOSE_TIMEOUT_SECONDS", DEFAULT_CLOSE_TIMEOUT_SECONDS, float
        )
        return cls(
            server_limits=env.server_limits(timeout),
            store_limits=env.store_limits(),
            session_limits=env.session_limits(timeout),
            max_subscriptions=env.read(
                "MAX_SUBSCRIPTIONS", DEFAULT_MAX_SUBSCRIPTIONS, int
            ),
            sweep_interval_ms=env.read("SWEEP_MS", DEFAULT_SWEEP_INTERVAL_MS, int),
            log_level=env.read("LOG_LEVEL", DEFAULT_LOG_LEVEL, normalize_log_level),
        )


class _Environment:
    """Parse one explicit environment mapping into independent component settings."""

    def __init__(self, source: Mapping[str, str]) -> None:
        self._source = source

    def read[T](self, name: str, default: T, parse: Callable[[str], T]) -> T:
        variable = f"VELIS_{name}"
        raw = self._source.get(variable)
        if raw is None:
            return default
        try:
            return parse(raw)
        except ValueError as exc:
            raise ValueError(f"{variable}: {exc}") from exc

    def server_limits(self, timeout: float) -> ServerLimits:
        defaults = DEFAULT_SERVER_LIMITS
        return ServerLimits(
            host=self.read("HOST", defaults.host, str),
            port=self.read("PORT", defaults.port, int),
            max_buffer=self.read("MAX_BUFFER", defaults.max_buffer, int),
            max_connections=self.read("MAX_CONNECTIONS", defaults.max_connections, int),
            close_timeout_seconds=timeout,
        )

    def store_limits(self) -> StoreLimits:
        defaults = DEFAULT_STORE_LIMITS
        return StoreLimits(
            max_keys=self.read("MAX_KEYS", defaults.max_keys, int),
            max_bytes=self.read("MAX_BYTES", defaults.max_bytes, int),
            eviction_policy=self.read(
                "EVICTION_POLICY", defaults.eviction_policy, EvictionPolicy
            ),
        )

    def session_limits(self, timeout: float) -> SessionLimits:
        defaults = DEFAULT_SESSION_LIMITS
        return SessionLimits(
            max_pending_requests=self.read(
                "MAX_PENDING_REQUESTS", defaults.max_pending_requests, int
            ),
            max_pending_messages=self.read(
                "MAX_PENDING_MESSAGES", defaults.max_pending_messages, int
            ),
            max_pending_bytes=self.read(
                "MAX_PENDING_BYTES", defaults.max_pending_bytes, int
            ),
            max_batch_bytes=self.read("MAX_BATCH_BYTES", defaults.max_batch_bytes, int),
            close_timeout_seconds=timeout,
        )
