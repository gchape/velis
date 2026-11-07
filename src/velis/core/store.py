from __future__ import annotations

import heapq

from velis.core.clock import Clock, SystemClock

MAX_TTL_MS = 1 << 62


class Store:
    """Store opaque bytes and monotonic TTLs on one owning thread."""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock: Clock = SystemClock if clock is None else clock
        self._values: dict[str, bytes] = {}
        self._deadlines: dict[str, int] = {}
        self._expiry_heap: list[tuple[int, str]] = []

    def __len__(self) -> int:
        """Return the stored key count without purging expired entries."""
        return len(self._values)

    def _remove(self, key: str) -> None:
        self._values.pop(key, None)
        self._deadlines.pop(key, None)

    def _remove_if_expired(self, key: str) -> None:
        deadline = self._deadlines.get(key)
        if deadline is not None and deadline <= self._clock.now_ms():
            self._remove(key)

    def get(self, key: str) -> bytes | None:
        """Return a live value, or None if the key is missing or expired."""
        self._remove_if_expired(key)
        return self._values.get(key)

    def set(self, key: str, value: bytes) -> None:
        """Store a value and clear any previous TTL."""
        self._values[key] = value
        self._deadlines.pop(key, None)
        self._compact_expiry_heap()

    def expire(self, key: str, ms: int) -> bool:
        """Set a relative TTL on a live key; expired keys stay missing.

        Raise ValueError for TTLs outside 0..MAX_TTL_MS, even for missing keys.
        Invalid TTLs leave the value and its existing expiration unchanged.
        """
        if not 0 <= ms <= MAX_TTL_MS:
            raise ValueError("ttl out of range")
        self._remove_if_expired(key)
        if key not in self._values:
            return False
        deadline = self._clock.now_ms() + ms
        self._deadlines[key] = deadline
        heapq.heappush(self._expiry_heap, (deadline, key))
        self._compact_expiry_heap()
        return True

    def _compact_expiry_heap(self) -> None:
        """Bound stale expiry entries relative to the number of active TTLs."""
        if len(self._expiry_heap) > max(1024, 2 * len(self._deadlines)):
            self._expiry_heap = [
                (deadline, key) for key, deadline in self._deadlines.items()
            ]
            heapq.heapify(self._expiry_heap)

    def sweep(self, limit: int = 1000) -> int:
        """Remove due keys and return the number removed.

        Process at most limit heap entries, including stale entries left by
        overwritten values or TTLs. Rebuild an oversized heap from current TTLs.
        """
        now_ms = self._clock.now_ms()
        removed = 0
        for _ in range(limit):
            if not self._expiry_heap or self._expiry_heap[0][0] > now_ms:
                break
            deadline, key = heapq.heappop(self._expiry_heap)
            if self._deadlines.get(key) == deadline:
                self._remove(key)
                removed += 1
        self._compact_expiry_heap()
        return removed
