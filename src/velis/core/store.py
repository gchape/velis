from dataclasses import dataclass

from cachetools import Cache, FIFOCache, LFUCache, LRUCache, RRCache

from velis.core.clock import Clock, SystemClock
from velis.core.expiry import MISSING, NO_EXPIRY, ExpiryIndex
from velis.errors import CapacityError, CommandError

MAX_TTL_MS = 1 << 62
DEFAULT_MAX_KEYS = 100_000
DEFAULT_MAX_BYTES = 64 << 20
DEFAULT_EVICTION_POLICY = "lru"


class _Reject(Cache[str, bytes]):
    def popitem(self) -> tuple[str, bytes]:
        raise CapacityError("store limit exceeded")


_CACHE_FACTORIES: dict[str, type[Cache[str, bytes]]] = {
    "reject": _Reject,
    "lru": LRUCache,
    "lfu": LFUCache,
    "fifo": FIFOCache,
    "rr": RRCache,
}
EVICTION_POLICIES = tuple(_CACHE_FACTORIES)


@dataclass(frozen=True, slots=True)
class StoreLimits:
    """Limits on retained keys and UTF-8 key plus value bytes."""

    max_keys: int = DEFAULT_MAX_KEYS
    max_bytes: int = DEFAULT_MAX_BYTES
    eviction_policy: str = DEFAULT_EVICTION_POLICY

    def __post_init__(self) -> None:
        if self.max_keys <= 0:
            raise ValueError(f"invalid max_keys: {self.max_keys}")
        if self.max_bytes <= 0:
            raise ValueError(f"invalid max_bytes: {self.max_bytes}")
        if self.eviction_policy not in _CACHE_FACTORIES:
            raise ValueError(f"invalid eviction_policy: {self.eviction_policy}")


def _entry_size(key: str, value: bytes) -> int:
    return len(key.encode()) + len(value)


class Store:
    """Store opaque bytes and monotonic TTLs on one owning thread.

    Reads and successful writes update the eviction policy. TTL inspection,
    expiry changes and persistence do not count as accesses.
    """

    def __init__(
        self,
        clock: Clock | None = None,
        *,
        limits: StoreLimits | None = None,
        max_keys: int | None = None,
        max_bytes: int | None = None,
        eviction_policy: str | None = None,
    ) -> None:
        # Keep the direct keyword API; all validation belongs to StoreLimits.
        if limits is None:
            limits = StoreLimits(
                DEFAULT_MAX_KEYS if max_keys is None else max_keys,
                DEFAULT_MAX_BYTES if max_bytes is None else max_bytes,
                DEFAULT_EVICTION_POLICY if eviction_policy is None else eviction_policy,
            )
        elif any(value is not None for value in (max_keys, max_bytes, eviction_policy)):
            raise ValueError("pass limits or individual store settings, not both")
        self._clock: Clock = SystemClock() if clock is None else clock
        self._limits = limits
        self._stored_bytes = 0
        self._sizes: dict[str, int] = {}
        self._values = _CACHE_FACTORIES[limits.eviction_policy](limits.max_keys)
        self._expiry = ExpiryIndex()

    def __len__(self) -> int:
        """Return the stored key count without purging expired entries."""
        return len(self._values)

    def _remove(self, key: str) -> bool:
        self._expiry.clear(key)
        if key not in self._values:
            return False
        del self._values[key]
        self._stored_bytes -= self._sizes.pop(key)
        return True

    @property
    def stored_bytes(self) -> int:
        """Retained UTF-8 key and value bytes, excluding Python object overhead."""
        return self._stored_bytes

    @property
    def has_due_expirations(self) -> bool:
        """Whether another bounded sweep can process due or stale heap entries."""
        return self._expiry.has_due(self._clock.now_ms())

    def _remove_if_expired(self, key: str, *, now: int | None = None) -> None:
        deadline = self._expiry.deadline(key)
        if deadline is not None and deadline <= (
            self._clock.now_ms() if now is None else now
        ):
            self._remove(key)

    def get(self, key: str) -> bytes | None:
        """Return a live value, or None if the key is missing or expired."""
        self._remove_if_expired(key)
        return self._values.get(key)

    def _exceeds_limits(self, key: str, size: int) -> bool:
        return (
            len(self._values) + (key not in self._sizes) > self._limits.max_keys
            or self._stored_bytes - self._sizes.get(key, 0) + size
            > self._limits.max_bytes
        )

    def _make_room(self, key: str, size: int) -> None:
        if self._exceeds_limits(key, size) and self.has_due_expirations:
            self.sweep()
        while self._exceeds_limits(key, size):
            victim, _ = self._values.popitem()
            self._stored_bytes -= self._sizes.pop(victim)
            self._expiry.clear(victim)

    def set(self, key: str, value: bytes) -> None:
        """Store a value and clear its TTL, applying the capacity policy.

        Reject oversized entries before changing values. Under capacity pressure,
        every policy runs one bounded expiry sweep before evicting or refusing.
        """
        size = _entry_size(key, value)
        if size > self._limits.max_bytes:
            raise CapacityError("store byte limit exceeded")
        self._remove_if_expired(key)
        self._make_room(key, size)
        self._values[key] = value
        self._stored_bytes += size - self._sizes.get(key, 0)
        self._sizes[key] = size
        self._expiry.clear(key)
        self._expiry.compact()

    def expire(self, key: str, ms: int) -> bool:
        """Set a relative TTL on a live key; expired keys stay missing.

        Raise CommandError for TTLs outside 0..MAX_TTL_MS, even for missing keys.
        Invalid TTLs leave the value and its existing expiration unchanged.
        """
        if not 0 <= ms <= MAX_TTL_MS:
            raise CommandError("ttl out of range")
        self._remove_if_expired(key)
        if key not in self._values:
            return False
        self._expiry.set(key, self._clock.now_ms() + ms)
        return True

    def delete(self, key: str) -> bool:
        """Delete a live key; return False for missing or expired keys."""
        self._remove_if_expired(key)
        removed = self._remove(key)
        self._expiry.compact()
        return removed

    def ttl(self, key: str) -> int:
        """Remaining milliseconds; MISSING (-2) or NO_EXPIRY (-1) otherwise."""
        now = self._clock.now_ms()
        self._remove_if_expired(key, now=now)
        if key not in self._values:
            return MISSING
        deadline = self._expiry.deadline(key)
        return NO_EXPIRY if deadline is None else deadline - now

    def persist(self, key: str) -> bool:
        """Remove a live key's TTL; return whether a deadline was removed."""
        self._remove_if_expired(key)
        changed = self._expiry.clear(key)
        self._expiry.compact()
        return changed

    def sweep(self, limit: int = 1000) -> int:
        """Remove due keys, processing at most limit heap entries, including stale."""
        return sum(
            self._remove(key)
            for key in self._expiry.pop_due(self._clock.now_ms(), limit)
        )
