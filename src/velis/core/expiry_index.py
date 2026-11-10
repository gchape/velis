"""Expiry deadlines in a min-heap with lazy deletion."""

from heapq import heapify, heappop, heappush

_COMPACT_SLACK = 64


class ExpiryIndex:
    """Keep exact deadlines and return keys once they are due.

    ``_deadlines`` is the truth. A heap entry is live only while it matches
    ``_deadlines``; cleared or rescheduled entries stay in the heap as garbage
    and are skipped when they reach the top. The heap is rebuilt when its size
    exceeds twice the live timers plus 64 entries, including after
    cancellations and sweeps. ``set`` is amortized O(log n); cancellation is
    amortized O(1), with occasional O(n) compaction.
    """

    def __init__(self) -> None:
        self._deadlines: dict[str, int] = {}
        self._heap: list[tuple[int, str]] = []

    def deadline(self, key: str) -> int | None:
        """Return the current deadline, or None when no timer exists."""
        return self._deadlines.get(key)

    def set(self, key: str, deadline: int) -> None:
        """Schedule a deadline, replacing any previous one."""
        if self._deadlines.get(key) == deadline:
            return
        self._deadlines[key] = deadline
        heappush(self._heap, (deadline, key))
        self._compact()

    def clear(self, key: str) -> bool:
        """Cancel a timer and report whether one existed. Missing is harmless."""
        if self._deadlines.pop(key, None) is None:
            return False
        self._compact()
        return True

    def has_due(self, now: int) -> bool:
        """Whether a live key's deadline has passed. Exact, never stale."""
        self._trim(now)
        return bool(self._heap) and self._heap[0][0] <= now

    def pop_due(self, now: int, limit: int) -> list[str]:
        """Remove and return up to limit due keys, earliest deadline first."""
        due: list[str] = []
        while len(due) < limit and self.has_due(now):
            _, key = heappop(self._heap)
            del self._deadlines[key]
            due.append(key)
        self._compact()
        return due

    def _compact(self) -> None:
        """Keep stale timers bounded even when the resident key set shrinks."""
        if len(self._heap) > 2 * len(self._deadlines) + _COMPACT_SLACK:
            self._heap = [(d, k) for k, d in self._deadlines.items()]
            heapify(self._heap)

    def _trim(self, now: int) -> None:
        """Drop stale due heads; future entries cannot hide a due timer."""
        heap, deadlines = self._heap, self._deadlines
        while heap and heap[0][0] <= now and deadlines.get(heap[0][1]) != heap[0][0]:
            heappop(heap)
