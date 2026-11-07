from __future__ import annotations

import heapq

from velis.core.clock import Clock, SystemClock


class Store:
    """Store opaque bytes and wall-clock TTLs on one owning thread."""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock: Clock = SystemClock if clock is None else clock
        self._data: dict[str, bytes] = {}
        self._deadline: dict[str, int] = {}
        self._heap: list[tuple[int, str]] = []

    def __len__(self) -> int:
        """Return the stored key count without purging expired entries."""
        return len(self._data)

    def _remove(self, key: str) -> None:
        self._data.pop(key, None)
        self._deadline.pop(key, None)

    def _purge_if_due(self, key: str) -> None:
        deadline = self._deadline.get(key)
        if deadline is not None and deadline <= self._clock.now_ms():
            self._remove(key)

    def get(self, key: str) -> bytes | None:
        """Return a live value, or None if the key is missing or expired."""
        self._purge_if_due(key)
        return self._data.get(key)

    def set(self, key: str, value: bytes) -> None:
        """Store a value and clear any previous TTL."""
        self._data[key] = value
        self._deadline.pop(key, None)

    def expire(self, key: str, ms: int) -> bool:
        """Set a relative TTL on a live key; expired keys stay missing."""
        self._purge_if_due(key)
        if key not in self._data:
            return False
        at_ms = self._clock.now_ms() + ms
        self._deadline[key] = at_ms
        heapq.heappush(self._heap, (at_ms, key))
        return True

    def sweep(self, limit: int = 1000) -> int:
        """Remove due keys and return the number removed.

        Process at most limit heap entries, including stale entries left by
        overwritten values or TTLs. Ignore deadlines that are no longer current.
        """
        now = self._clock.now_ms()
        removed = 0
        for _ in range(limit):
            if not self._heap or self._heap[0][0] > now:
                break
            deadline, key = heapq.heappop(self._heap)
            if self._deadline.get(key) == deadline:
                self._remove(key)
                removed += 1
        return removed
