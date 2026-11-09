"""Expiry deadlines in a min-heap with lazy deletion."""

from heapq import heapify, heappop, heappush

_COMPACT_SLACK = 64


class ExpiryIndex:
    """Keep exact deadlines and return keys once they are due.

    ``_deadlines`` is the truth. A heap entry is live only while it matches
    ``_deadlines``; cleared or rescheduled entries stay in the heap as garbage
    and are skipped when they reach the top. The heap is rebuilt when garbage
    exceeds roughly twice the live timers plus 64 entries, so ``set`` is
    amortized O(log n) and ``clear`` is O(1).
    """

    def __init__(self) -> None:
        self._deadlines: dict[str, int] = {}
        self._heap: list[tuple[int, str]] = []

    def deadline(self, key: str) -> int | None:
        """Return the current deadline, or None when no timer exists."""
        return self._deadlines.get(key)

    def set(self, key: str, deadline: int) -> None:
        """Schedule a deadline, replacing any previous one."""
        self._deadlines[key] = deadline
        heappush(self._heap, (deadline, key))
        if len(self._heap) > 2 * len(self._deadlines) + _COMPACT_SLACK:
            self._heap = [(d, k) for k, d in self._deadlines.items()]
            heapify(self._heap)

    def clear(self, key: str) -> bool:
        """Cancel a timer and report whether one existed. Missing is harmless."""
        return self._deadlines.pop(key, None) is not None

    def has_due(self, now: int) -> bool:
        """Whether a live key's deadline has passed. Exact, never stale."""
        self._trim()
        return bool(self._heap) and self._heap[0][0] <= now

    def pop_due(self, now: int, limit: int) -> list[str]:
        """Remove and return up to limit due keys, earliest deadline first."""
        due: list[str] = []
        while len(due) < limit and self.has_due(now):
            _, key = heappop(self._heap)
            del self._deadlines[key]
            due.append(key)
        return due

    def _trim(self) -> None:
        """Drop heap heads whose key was cleared or rescheduled."""
        heap, deadlines = self._heap, self._deadlines
        while heap and deadlines.get(heap[0][1]) != heap[0][0]:
            heappop(heap)
