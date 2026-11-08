"""Bounded monotonic expiry indexing, independent of stored values."""

import heapq

MISSING = -2
NO_EXPIRY = -1


class ExpiryIndex:
    """Index current deadlines while tolerating stale heap entries."""

    def __init__(self) -> None:
        self._deadlines: dict[str, int] = {}
        self._heap: list[tuple[int, str]] = []

    def deadline(self, key: str) -> int | None:
        return self._deadlines.get(key)

    def set(self, key: str, deadline: int) -> None:
        self._deadlines[key] = deadline
        heapq.heappush(self._heap, (deadline, key))
        self.compact()

    def clear(self, key: str) -> bool:
        """Clear a deadline; callers can compact after a batch of removals."""
        return self._deadlines.pop(key, None) is not None

    def has_due(self, now: int) -> bool:
        """Include stale due entries so maintenance can drain bounded batches."""
        return bool(self._heap and self._heap[0][0] <= now)

    def pop_due(self, now: int, limit: int) -> list[str]:
        """Consume at most limit heap entries, returning current expired keys."""
        due = []
        for _ in range(limit):
            if not self.has_due(now):
                break
            deadline, key = heapq.heappop(self._heap)
            if self._deadlines.get(key) == deadline:
                del self._deadlines[key]
                due.append(key)
        self.compact()
        return due

    def compact(self) -> None:
        """Bound stale entries relative to active deadlines."""
        if len(self._heap) > max(1024, 2 * len(self._deadlines)):
            self._heap = [(deadline, key) for key, deadline in self._deadlines.items()]
            heapq.heapify(self._heap)
