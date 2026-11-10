"""Immutable component limits; independent of environment loading and runtime I/O."""

from dataclasses import dataclass

from velis.core.eviction import EvictionPolicy
from velis.validation import require_positive, require_positive_int

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
DEFAULT_MAX_PENDING_REQUESTS = 64
DEFAULT_MAX_PENDING_MESSAGES = 1024
DEFAULT_MAX_PENDING_BYTES = 8 << 20
DEFAULT_MAX_BATCH_BYTES = 256 << 10
DEFAULT_CLOSE_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class ServerLimits:
    """TCP listener, connection and subscriber buffer settings."""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    max_buffer: int = DEFAULT_MAX_BUFFER
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    close_timeout_seconds: float = DEFAULT_CLOSE_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        if type(self.port) is not int or not 0 <= self.port <= 65535:
            raise ValueError(f"invalid port: {self.port}")
        require_positive_int(
            max_buffer=self.max_buffer,
            max_connections=self.max_connections,
        )
        require_positive(close_timeout_seconds=self.close_timeout_seconds)


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
        require_positive_int(max_keys=self.max_keys, max_bytes=self.max_bytes)
        object.__setattr__(
            self, "eviction_policy", EvictionPolicy(self.eviction_policy)
        )


@dataclass(frozen=True, slots=True)
class SessionLimits:
    """Bounds on in-flight requests, batch writes and unread client messages.

    The batch byte target includes framing. One valid command may exceed it
    and is then written alone, so reducing the target never rejects a command.
    """

    max_pending_requests: int = DEFAULT_MAX_PENDING_REQUESTS
    max_pending_messages: int = DEFAULT_MAX_PENDING_MESSAGES
    max_pending_bytes: int = DEFAULT_MAX_PENDING_BYTES
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES
    close_timeout_seconds: float = DEFAULT_CLOSE_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        require_positive_int(
            max_pending_requests=self.max_pending_requests,
            max_pending_messages=self.max_pending_messages,
            max_pending_bytes=self.max_pending_bytes,
            max_batch_bytes=self.max_batch_bytes,
        )
        require_positive(close_timeout_seconds=self.close_timeout_seconds)


DEFAULT_SERVER_LIMITS = ServerLimits()
DEFAULT_STORE_LIMITS = StoreLimits()
DEFAULT_SESSION_LIMITS = SessionLimits()
