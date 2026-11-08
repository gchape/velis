from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import NamedTuple

from velis.core.clock import Clock, SystemClock
from velis.core.commands import MISSING, NO_EXPIRY
from velis.core.eviction import Eviction, Fifo, Lfu, Lru, Random
from velis.core.timing_wheel import TimingWheel
from velis.errors import CapacityError, CommandError
from velis.validation import require_positive

MAX_TTL_MS = 1 << 62
DEFAULT_MAX_KEYS = 100_000
DEFAULT_MAX_BYTES = 64 << 20


class _Reject:
    """Null object: never evicts, so a full store refuses the write."""

    def access(self, key: str) -> None: ...

    def discard(self, key: str) -> None: ...

    def victim(self) -> str:
        raise CapacityError("store limit exceeded")


class EvictionPolicy(StrEnum):
    REJECT = "reject"
    LRU = "lru"
    FIFO = "fifo"
    LFU = "lfu"
    RANDOM = "random"

    def create(self) -> Eviction:
        return _POLICIES[self]()


_POLICIES: dict[EvictionPolicy, Callable[[], Eviction]] = {
    EvictionPolicy.REJECT: _Reject,
    EvictionPolicy.LRU: Lru,
    EvictionPolicy.FIFO: Fifo,
    EvictionPolicy.LFU: Lfu,
    EvictionPolicy.RANDOM: Random,
}

DEFAULT_EVICTION_POLICY = EvictionPolicy.LRU


@dataclass(frozen=True, slots=True)
class StoreLimits:
    """Bound retained keys and UTF-8 key plus value bytes.

    Policy string values are normalized to EvictionPolicy during construction.
    """

    max_keys: int = DEFAULT_MAX_KEYS
    max_bytes: int = DEFAULT_MAX_BYTES
    eviction_policy: EvictionPolicy = DEFAULT_EVICTION_POLICY

    def __post_init__(self) -> None:
        require_positive("max_keys", self.max_keys)
        require_positive("max_bytes", self.max_bytes)
        object.__setattr__(
            self, "eviction_policy", EvictionPolicy(self.eviction_policy)
        )


DEFAULT_LIMITS = StoreLimits()


class _Entry(NamedTuple):
    value: bytes
    size: int  # UTF-8 key bytes + value bytes, computed once at write time


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
        limits: StoreLimits = DEFAULT_LIMITS,
    ) -> None:
        self._clock: Clock = SystemClock() if clock is None else clock
        self._limits = limits
        self._entries: dict[str, _Entry] = {}
        self._stored_bytes = 0
        self._eviction = limits.eviction_policy.create()
        self._expiry = TimingWheel(self._clock.now_ms())

    def __len__(self) -> int:
        """Return the stored key count without purging expired entries."""
        return len(self._entries)

    @property
    def stored_bytes(self) -> int:
        """Retained UTF-8 key and value bytes, excluding Python object overhead."""
        return self._stored_bytes

    @property
    def has_due_expirations(self) -> bool:
        """Whether due timers or wheel cascades need another bounded sweep."""
        return self._expiry.has_due(self._clock.now_ms())

    def _remove(self, key: str) -> bool:
        """Remove a value, its timer, size accounting and eviction metadata."""
        self._expiry.clear(key)
        entry = self._entries.pop(key, None)
        if entry is None:
            return False
        self._stored_bytes -= entry.size
        self._eviction.discard(key)
        return True

    def _remove_if_expired(self, key: str, *, now: int | None = None) -> None:
        deadline = self._expiry.deadline(key)
        if deadline is None:
            return
        if deadline <= (self._clock.now_ms() if now is None else now):
            self._remove(key)

    def _would_exceed(self, key: str, size: int) -> bool:
        old = self._entries.get(key)
        freed = 0 if old is None else old.size
        return (
            len(self._entries) + (old is None) > self._limits.max_keys
            or self._stored_bytes - freed + size > self._limits.max_bytes
        )

    def _make_room(self, key: str, size: int) -> None:
        if self._would_exceed(key, size) and self.has_due_expirations:
            self.sweep()
        while self._would_exceed(key, size):
            self._remove(self._eviction.victim())

    def get(self, key: str) -> bytes | None:
        """Return a live value, or None if the key is missing or expired."""
        self._remove_if_expired(key)
        entry = self._entries.get(key)
        if entry is None:
            return None
        self._eviction.access(key)
        return entry.value

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
        old = self._entries.get(key)  # read after: making room may evict this key
        self._entries[key] = _Entry(value, size)
        self._stored_bytes += size - (0 if old is None else old.size)
        self._expiry.clear(key)
        self._eviction.access(key)

    def expire(self, key: str, ms: int) -> bool:
        """Set a relative TTL on a live key; expired keys stay missing.

        Raise CommandError for TTLs outside 0..MAX_TTL_MS, even for missing keys.
        Invalid TTLs leave the value and its existing expiration unchanged.
        """
        if not 0 <= ms <= MAX_TTL_MS:
            raise CommandError("ttl out of range")
        self._remove_if_expired(key)
        if key not in self._entries:
            return False
        self._expiry.set(key, self._clock.now_ms() + ms)
        return True

    def delete(self, key: str) -> bool:
        """Delete a live key; return False for missing or expired keys."""
        self._remove_if_expired(key)
        return self._remove(key)

    def ttl(self, key: str) -> int:
        """Remaining milliseconds; MISSING (-2) or NO_EXPIRY (-1) otherwise."""
        now = self._clock.now_ms()
        self._remove_if_expired(key, now=now)
        if key not in self._entries:
            return MISSING
        deadline = self._expiry.deadline(key)
        return NO_EXPIRY if deadline is None else deadline - now

    def persist(self, key: str) -> bool:
        """Remove a live key's TTL; return whether a deadline was removed."""
        self._remove_if_expired(key)
        return self._expiry.clear(key)

    def sweep(self, limit: int = 1000) -> int:
        """Remove due keys, processing at most limit timers, including cascades."""
        return sum(
            self._remove(key)
            for key in self._expiry.pop_due(self._clock.now_ms(), limit)
        )
