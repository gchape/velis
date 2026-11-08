from dataclasses import dataclass
from enum import StrEnum

from velis.core.clock import Clock, SystemClock
from velis.core.commands import MISSING, NO_EXPIRY
from velis.core.eviction import Eviction, Lru, Sieve
from velis.core.timing_wheel import TimingWheel
from velis.errors import CapacityError, CommandError
from velis.validation import require_positive

MAX_TTL_MS = 1 << 62
DEFAULT_MAX_KEYS = 100_000
DEFAULT_MAX_BYTES = 64 << 20


class EvictionPolicy(StrEnum):
    REJECT = "reject"
    LRU = "lru"
    SIEVE = "sieve"


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
        try:
            policy = EvictionPolicy(self.eviction_policy)
        except ValueError:
            raise ValueError(
                f"invalid eviction_policy: {self.eviction_policy!r}"
            ) from None
        object.__setattr__(self, "eviction_policy", policy)


DEFAULT_LIMITS = StoreLimits()


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
        self._stored_bytes = 0
        self._sizes: dict[str, int] = {}
        self._values: dict[str, bytes] = {}
        self._eviction: Eviction | None = None
        if limits.eviction_policy == EvictionPolicy.LRU:
            self._eviction = Lru()
        elif limits.eviction_policy == EvictionPolicy.SIEVE:
            self._eviction = Sieve()
        self._expiry = TimingWheel(self._clock.now_ms())

    def __len__(self) -> int:
        """Return the stored key count without purging expired entries."""
        return len(self._values)

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
        if key not in self._values:
            return False
        del self._values[key]
        self._stored_bytes -= self._sizes.pop(key)
        if self._eviction is not None:
            self._eviction.discard(key)
        return True

    def _remove_if_expired(self, key: str, *, now: int | None = None) -> None:
        deadline = self._expiry.deadline(key)
        if deadline is not None and deadline <= (
            self._clock.now_ms() if now is None else now
        ):
            self._remove(key)

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
            if self._eviction is None:
                raise CapacityError("store limit exceeded")
            self._remove(self._eviction.victim())

    def get(self, key: str) -> bytes | None:
        """Return a live value, or None if the key is missing or expired."""
        self._remove_if_expired(key)
        value = self._values.get(key)
        if value is not None and self._eviction is not None:
            self._eviction.access(key)
        return value

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
        if self._eviction is not None:
            self._eviction.access(key)

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
        return self._remove(key)

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
        return self._expiry.clear(key)

    def sweep(self, limit: int = 1000) -> int:
        """Remove due keys, processing at most limit timers, including cascades."""
        return sum(
            self._remove(key)
            for key in self._expiry.pop_due(self._clock.now_ms(), limit)
        )
