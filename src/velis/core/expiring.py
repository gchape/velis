"""TTL layer: deadlines, purge-on-access and bounded sweeps."""

from enum import Enum, auto

from velis.core.clock import Clock
from velis.core.evicting import Evicting
from velis.core.expiry_index import ExpiryIndex
from velis.errors import CommandError

MAX_TTL_MS = 1 << 62
SWEEP_LIMIT = 1000


class TtlStatus(Enum):
    """Why a TTL query has no remaining-milliseconds value."""

    MISSING = auto()
    PERSISTENT = auto()


class Expiring:
    """Add monotonic TTLs on top of an Evicting layer.

    Expired keys leave through ``Evicting.delete``, so the eviction policy
    never holds a dead key. Capacity victims clear their own timer through
    the ``on_evict`` hook, and a full store sweeps expired keys through the
    ``reclaim`` hook before evicting live ones.

    Invariant: every key with a timer is resident.
    Operations that check expiry sample the clock once.
    """

    def __init__(self, inner: Evicting, clock: Clock) -> None:
        self._inner = inner
        self._clock = clock
        self._expiry = ExpiryIndex()
        self.expirations = 0
        inner.on_evict = self._expiry.clear
        inner.reclaim = self.sweep

    def __len__(self) -> int:
        """Stored key count, without purging expired entries."""
        return len(self._inner)

    @property
    def stored_bytes(self) -> int:
        return self._inner.stored_bytes

    @property
    def evictions(self) -> int:
        return self._inner.evictions

    @property
    def has_due_expirations(self) -> bool:
        return self._expiry.has_due(self._clock.now_ms())

    def get(self, key: str) -> bytes | None:
        self._purge(key, self._clock.now_ms())
        return self._inner.get(key)

    def set(self, key: str, value: bytes) -> None:
        """Store a value and clear its TTL.

        An oversized value raises before anything changes. A successful
        overwrite of an expired predecessor counts as an expiration.
        """
        now = self._clock.now_ms()
        deadline = self._expiry.deadline(key)
        self._inner.set(key, value)
        self._expiry.clear(key)
        if deadline is not None and deadline <= now:
            self.expirations += 1

    def delete(self, key: str) -> bool:
        """Delete a live key; False for missing or expired keys."""
        self._purge(key, self._clock.now_ms())
        self._expiry.clear(key)
        return self._inner.delete(key)

    def expire(self, key: str, ms: int) -> bool:
        """Set a relative TTL on a live key; expired keys stay missing.

        Raise CommandError for TTLs outside 0..MAX_TTL_MS, even for missing
        keys. An invalid TTL leaves any existing expiration unchanged.
        """
        if not 0 <= ms <= MAX_TTL_MS:
            raise CommandError("ttl out of range")
        now = self._clock.now_ms()
        self._purge(key, now)
        if key not in self._inner:
            return False
        self._expiry.set(key, now + ms)
        return True

    def ttl(self, key: str) -> int | TtlStatus:
        """Remaining milliseconds, or a TtlStatus when there is no deadline."""
        now = self._clock.now_ms()
        self._purge(key, now)
        if key not in self._inner:
            return TtlStatus.MISSING
        deadline = self._expiry.deadline(key)
        return TtlStatus.PERSISTENT if deadline is None else deadline - now

    def persist(self, key: str) -> bool:
        """Remove a live key's TTL; return whether a deadline was removed."""
        self._purge(key, self._clock.now_ms())
        return self._expiry.clear(key)

    def sweep(self, limit: int = SWEEP_LIMIT) -> int:
        """Remove up to limit expired keys and return how many were removed."""
        keys = self._expiry.pop_due(self._clock.now_ms(), limit)
        removed = sum(map(self._inner.delete, keys))
        self.expirations += removed
        return removed

    def _purge(self, key: str, now: int) -> None:
        deadline = self._expiry.deadline(key)
        if deadline is not None and deadline <= now:
            self._expiry.clear(key)
            self.expirations += self._inner.delete(key)
