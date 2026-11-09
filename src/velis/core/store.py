"""Capacity accounting and live-value access over eviction and expiry policies."""

from dataclasses import dataclass
from enum import Enum, auto
from typing import NamedTuple

from velis.core.clock import Clock, MonotonicClock
from velis.core.eviction import DEFAULT_EVICTION_POLICY, EvictionPolicy
from velis.core.expiry_index import ExpiryIndex
from velis.errors import CapacityError, CommandError
from velis.validation import require_positive

MAX_TTL_MS = 1 << 62
DEFAULT_MAX_KEYS = 100_000
DEFAULT_MAX_BYTES = 64 << 20
SWEEP_LIMIT = 1000


@dataclass(frozen=True, slots=True)
class StoreLimits:
    """Bound retained keys and UTF-8 key plus value bytes.

    Policy string values are normalized to EvictionPolicy during construction.
    The byte budget excludes Python objects and policy/expiry metadata; it is
    not a process-memory limit, especially for many small entries.
    """

    max_keys: int = DEFAULT_MAX_KEYS
    max_bytes: int = DEFAULT_MAX_BYTES
    eviction_policy: EvictionPolicy = DEFAULT_EVICTION_POLICY

    def __post_init__(self) -> None:
        require_positive(max_keys=self.max_keys, max_bytes=self.max_bytes)
        object.__setattr__(
            self, "eviction_policy", EvictionPolicy(self.eviction_policy)
        )


DEFAULT_STORE_LIMITS = StoreLimits()


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


class TtlStatus(Enum):
    """Why a TTL query has no remaining-milliseconds value."""

    MISSING = auto()
    PERSISTENT = auto()


class _Entry(NamedTuple):
    """Retain a value and its UTF-8 key plus value byte count, measured at SET."""

    value: bytes
    size: int


class Store:
    """Store opaque bytes and monotonic TTLs on one owning thread.

    Reads and successful writes update the eviction policy. TTL inspection,
    expiry changes and persistence do not count as accesses. Operations that
    check expiry sample the clock once, so their decisions share one instant.

    Invariant: every key with a timer is resident.
    """

    def __init__(
        self,
        clock: Clock | None = None,
        *,
        limits: StoreLimits = DEFAULT_STORE_LIMITS,
    ) -> None:
        self._clock: Clock = MonotonicClock() if clock is None else clock
        self._limits = limits
        self._entries: dict[str, _Entry] = {}
        self._stored_bytes = 0
        self._eviction = limits.eviction_policy.create(max_keys=limits.max_keys)
        self._expiry = ExpiryIndex()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expirations = 0

    def __len__(self) -> int:
        """Return the stored key count without purging expired entries."""
        return len(self._entries)

    @property
    def stored_bytes(self) -> int:
        """Retained UTF-8 key and value bytes, excluding Python object overhead."""
        return self._stored_bytes

    def stats(self) -> StoreStats:
        """Return an immutable snapshot of counters since construction."""
        return StoreStats(self._hits, self._misses, self._evictions, self._expirations)

    @property
    def has_due_expirations(self) -> bool:
        """Whether due keys or stale time ticks need another sweep."""
        return self._expiry.has_due(self._clock.now_ms())

    def get(self, key: str) -> bytes | None:
        """Return a live value, or None if the key is missing or expired."""
        self._purge(key, self._clock.now_ms())
        if (entry := self._entries.get(key)) is None:
            self._misses += 1
            return None
        self._hits += 1
        self._eviction.access(key)
        return entry.value

    def set(self, key: str, value: bytes) -> None:
        """Store a value and clear its TTL, applying the capacity policy.

        Reject oversized entries before changing anything. Under capacity
        pressure, one bounded expiry sweep runs before live entries are evicted.
        An expired predecessor loses its eviction history. A live overwrite
        frees its old value and timer before checking capacity for the new value.
        """
        size = len(key.encode()) + len(value)
        if size > self._limits.max_bytes:
            raise CapacityError("store byte limit exceeded")
        now = self._clock.now_ms()
        self._purge(key, now)
        self._drop(key)
        self._make_room(size, now)
        self._entries[key] = _Entry(value, size)
        self._stored_bytes += size
        self._eviction.access(key)

    def expire(self, key: str, ms: int) -> bool:
        """Set a relative TTL on a live key; expired keys stay missing.

        Raise CommandError for TTLs outside 0..MAX_TTL_MS, even for missing keys.
        Invalid TTLs leave the value and its existing expiration unchanged.
        """
        if not 0 <= ms <= MAX_TTL_MS:
            raise CommandError("ttl out of range")
        now = self._clock.now_ms()
        self._purge(key, now)
        if key not in self._entries:
            return False
        self._expiry.set(key, now + ms)
        return True

    def delete(self, key: str) -> bool:
        """Delete a live key; return False for missing or expired keys."""
        self._purge(key, self._clock.now_ms())
        return self._remove(key)

    def ttl(self, key: str) -> int | TtlStatus:
        """Remaining milliseconds, or a TtlStatus when there is no deadline."""
        now = self._clock.now_ms()
        self._purge(key, now)
        if key not in self._entries:
            return TtlStatus.MISSING
        deadline = self._expiry.deadline(key)
        return TtlStatus.PERSISTENT if deadline is None else deadline - now

    def persist(self, key: str) -> bool:
        """Remove a live key's TTL; return whether a deadline was removed."""
        self._purge(key, self._clock.now_ms())
        return self._expiry.clear(key)

    def sweep(self, limit: int = SWEEP_LIMIT) -> int:
        """Remove expired keys and return how many were removed.

        Spend at most limit units on due keys or stale time ticks. Stale ticks
        consume budget without increasing the return value. Tick-heap compaction
        can take O(n) in the number of occupied ticks.
        """
        return self._sweep(self._clock.now_ms(), limit)

    def _sweep(self, now: int, limit: int) -> int:
        removed = sum(map(self._remove, self._expiry.pop_due(now, limit)))
        self._expirations += removed
        return removed

    def _purge(self, key: str, now: int) -> None:
        """Remove the key everywhere if its deadline has passed."""
        deadline = self._expiry.deadline(key)
        if deadline is not None and deadline <= now:
            self._expirations += self._remove(key)

    def _drop(self, key: str) -> bool:
        """Remove value, size and timer; the eviction policy is the caller's job."""
        self._expiry.clear(key)
        if (entry := self._entries.pop(key, None)) is None:
            return False
        self._stored_bytes -= entry.size
        return True

    def _remove(self, key: str) -> bool:
        """Remove a key everywhere, including eviction metadata."""
        if not self._drop(key):
            return False
        self._eviction.discard(key)
        return True

    def _would_exceed(self, size: int) -> bool:
        """Whether adding one non-resident entry of this size breaks a limit."""
        return (
            len(self._entries) >= self._limits.max_keys
            or self._stored_bytes + size > self._limits.max_bytes
        )

    def _make_room(self, size: int, now: int) -> None:
        """Reclaim expired capacity, then evict until the new entry fits.

        A growing overwrite can select its own policy record after its previous
        value was dropped. Count only victims that still have stored values.
        """
        if self._would_exceed(size) and self._expiry.has_due(now):
            self._sweep(now, SWEEP_LIMIT)
        while self._would_exceed(size):
            self._evictions += self._drop(self._eviction.evict())
