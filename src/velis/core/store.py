"""Public store: wires Memory, Evicting and Expiring and counts reads."""

from dataclasses import dataclass

from velis.config import (
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_KEYS,
    DEFAULT_STORE_LIMITS,
    StoreLimits,
)
from velis.core.clock import Clock, MonotonicClock
from velis.core.evicting import Evicting
from velis.core.expiring import MAX_TTL_MS, SWEEP_LIMIT, Expiring, TtlStatus
from velis.core.kv import Memory

__all__ = [
    "DEFAULT_MAX_BYTES",
    "DEFAULT_MAX_KEYS",
    "DEFAULT_STORE_LIMITS",
    "MAX_TTL_MS",
    "SWEEP_LIMIT",
    "Store",
    "StoreLimits",
    "StoreStats",
    "TtlStatus",
]


@dataclass(frozen=True, slots=True)
class StoreStats:
    """Cumulative GET hits/misses, capacity evictions and removed expired keys.

    Expirations count physical removal, including lazy reads and write-time
    cleanup. Deleting or overwriting live keys does not increment either count.
    """

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0


class Store:
    """Store opaque bytes and monotonic TTLs on one owning thread.

    Layers, outermost first: Expiring (TTLs) over Evicting (capacity and
    policy) over Memory (dict and byte count). The order is load-bearing:
    expiry deletes through the eviction layer so the policy never keeps a
    dead key. Reads and successful writes update the eviction policy; TTL
    inspection, expiry changes and persistence do not.
    """

    def __init__(
        self,
        clock: Clock | None = None,
        *,
        limits: StoreLimits = DEFAULT_STORE_LIMITS,
    ) -> None:
        self._inner = Expiring(
            Evicting(Memory(), limits), MonotonicClock() if clock is None else clock
        )
        self._hits = 0
        self._misses = 0

    def __len__(self) -> int:
        return len(self._inner)

    @property
    def stored_bytes(self) -> int:
        return self._inner.stored_bytes

    @property
    def has_due_expirations(self) -> bool:
        return self._inner.has_due_expirations

    def stats(self) -> StoreStats:
        return StoreStats(
            self._hits,
            self._misses,
            self._inner.evictions,
            self._inner.expirations,
        )

    def get(self, key: str) -> bytes | None:
        value = self._inner.get(key)
        if value is None:
            self._misses += 1
        else:
            self._hits += 1
        return value

    def set(self, key: str, value: bytes) -> None:
        self._inner.set(key, value)

    def delete(self, key: str) -> bool:
        return self._inner.delete(key)

    def expire(self, key: str, ms: int) -> bool:
        return self._inner.expire(key, ms)

    def ttl(self, key: str) -> int | TtlStatus:
        return self._inner.ttl(key)

    def persist(self, key: str) -> bool:
        return self._inner.persist(key)

    def sweep(self, limit: int = SWEEP_LIMIT) -> int:
        return self._inner.sweep(limit)
