"""Public store: values, eviction policy and TTLs behind one invariant."""

from dataclasses import dataclass
from enum import Enum, auto

from velis.core.clock import Clock, MonotonicClock
from velis.core.eviction import EvictionPolicy
from velis.core.expiry_index import ExpiryIndex
from velis.errors import CapacityError, CommandError
from velis.validation import require_positive_int

__all__ = [
    "DEFAULT_EVICTION_POLICY",
    "DEFAULT_MAX_BYTES",
    "DEFAULT_MAX_KEYS",
    "DEFAULT_STORE_LIMITS",
    "DEFAULT_SWEEP_INTERVAL_MS",
    "MAX_TTL_MS",
    "SWEEP_LIMIT",
    "Store",
    "StoreLimits",
    "StoreStats",
    "TtlStatus",
]

MAX_TTL_MS = 1 << 62
SWEEP_LIMIT = 1000
DEFAULT_SWEEP_INTERVAL_MS = 100
DEFAULT_MAX_KEYS = 100_000
DEFAULT_MAX_BYTES = 64 << 20
DEFAULT_EVICTION_POLICY = EvictionPolicy.S3_FIFO


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


DEFAULT_STORE_LIMITS = StoreLimits()


class TtlStatus(Enum):
    """Why a TTL query has no remaining-milliseconds value."""

    MISSING = auto()
    PERSISTENT = auto()


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

    Three structures describe the same key set: ``_entries`` holds values and
    byte counts, the eviction policy orders resident keys, and ``ExpiryIndex``
    holds deadlines. They are never changed independently. Invariant: every
    key in the policy or the expiry index is resident, and every resident key
    is in the policy.

    A key leaves by exactly one of two paths. ``_remove`` is for removals the
    store chooses (delete, overwrite, expiry): the policy discards the key and
    any ghost history. ``_drop`` is for capacity victims, which the policy has
    already forgotten, keeping whatever ghost history it wants.

    Operations that check expiry sample the clock once. Reads and successful
    writes update the eviction policy; TTL inspection, expiry changes and
    persistence do not.
    """

    def __init__(
        self,
        clock: Clock | None = None,
        *,
        limits: StoreLimits = DEFAULT_STORE_LIMITS,
    ) -> None:
        self._clock = MonotonicClock() if clock is None else clock
        self._limits = limits
        self._entries: dict[str, tuple[bytes, int]] = {}
        self._stored_bytes = 0
        self._policy = limits.eviction_policy.create()
        self._expiry = ExpiryIndex()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expirations = 0

    def __len__(self) -> int:
        """Stored key count, without purging expired entries."""
        return len(self._entries)

    @property
    def stored_bytes(self) -> int:
        return self._stored_bytes

    @property
    def has_due_expirations(self) -> bool:
        return self._expiry.has_due(self._clock.now_ms())

    def stats(self) -> StoreStats:
        return StoreStats(self._hits, self._misses, self._evictions, self._expirations)

    def get(self, key: str) -> bytes | None:
        self._purge(key, self._clock.now_ms())
        entry = self._entries.get(key)
        if entry is None:
            self._misses += 1
            return None
        self._hits += 1
        self._policy.access(key)
        return entry[0]

    def set(self, key: str, value: bytes) -> None:
        """Store a value and clear its TTL, evicting if capacity requires.

        An oversized entry raises before anything changes. Overwriting an
        expired predecessor counts as an expiration; overwriting a live one
        restarts its eviction history and counts nothing. A non-resident key
        keeps any ghost history, so a recently evicted key can re-enter hot.
        """
        size = len(key.encode()) + len(value)
        if size > self._limits.max_bytes:
            raise CapacityError("store byte limit exceeded")
        now = self._clock.now_ms()
        self._purge(key, now)
        if key in self._entries:
            self._remove(key)
        if self._full(size):
            self._sweep(now, SWEEP_LIMIT)
        while self._full(size):
            self._drop(self._policy.evict())
            self._evictions += 1
        self._entries[key] = (value, size)
        self._stored_bytes += size
        self._policy.access(key)

    def delete(self, key: str) -> bool:
        """Delete a live key; False for missing or expired keys."""
        self._purge(key, self._clock.now_ms())
        return self._remove(key)

    def expire(self, key: str, ms: int) -> bool:
        """Set a relative TTL on a live key; expired keys stay missing.

        Raise CommandError for TTLs outside 0..MAX_TTL_MS, even for missing
        keys. An invalid TTL leaves any existing expiration unchanged.
        """
        if type(ms) is not int or not 0 <= ms <= MAX_TTL_MS:
            raise CommandError("ttl out of range")
        now = self._clock.now_ms()
        self._purge(key, now)
        if key not in self._entries:
            return False
        self._expiry.set(key, now + ms)
        return True

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
        """Remove up to limit expired keys and return how many were removed."""
        return self._sweep(self._clock.now_ms(), limit)

    def _full(self, size: int) -> bool:
        """Whether adding one non-resident entry of this size breaks a limit."""
        return (
            len(self._entries) >= self._limits.max_keys
            or self._stored_bytes + size > self._limits.max_bytes
        )

    def _remove(self, key: str) -> bool:
        """Remove a key the store chose to remove; the policy forgets it fully."""
        self._policy.discard(key)
        return self._drop(key)

    def _drop(self, key: str) -> bool:
        """Remove a key the policy has already forgotten, with its timer."""
        self._expiry.clear(key)
        entry = self._entries.pop(key, None)
        if entry is None:
            return False
        self._stored_bytes -= entry[1]
        return True

    def _expire(self, key: str) -> None:
        self._remove(key)
        self._expirations += 1

    def _purge(self, key: str, now: int) -> None:
        """Remove the key if its deadline has passed."""
        deadline = self._expiry.deadline(key)
        if deadline is not None and deadline <= now:
            self._expire(key)

    def _sweep(self, now: int, limit: int) -> int:
        keys = self._expiry.pop_due(now, limit)
        for key in keys:
            self._expire(key)
        return len(keys)
