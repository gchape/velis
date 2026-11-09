"""Expiry deadlines bucketed by time tick, with real removal on cancellation."""

from heapq import heapify, heappop, heappush

from velis.validation import require_positive

_COMPACT_SLACK = 64


class ExpiryIndex:
    """Keep exact deadlines and return keys once their time bucket is due.

    Buckets cover tick_ms milliseconds and become due at their last millisecond.
    No key is returned early; larger ticks delay eligibility by less than one
    tick. The default tick_ms=1 preserves exact deadlines, including negative or
    arbitrarily large clock origins. Nonpositive tick sizes raise ValueError.

    Replacing or clearing a timer removes its key from the old bucket immediately.
    Empty or recreated buckets may leave stale or duplicate tick IDs, so readiness
    can be true even when a sweep returns no keys. Repeated deadlines still return
    each key once. The tick heap compacts above twice the occupied bucket count
    plus 64 IDs; this takes linear time in the occupied bucket count.
    """

    def __init__(self, tick_ms: int = 1) -> None:
        require_positive(tick_ms=tick_ms)
        self._tick_ms = tick_ms
        self._deadlines: dict[str, int] = {}
        self._buckets: dict[int, set[str]] = {}
        self._ticks: list[int] = []

    def deadline(self, key: str) -> int | None:
        """Return the current deadline, or None when no timer exists."""
        return self._deadlines.get(key)

    def set(self, key: str, deadline: int) -> None:
        """Schedule a deadline, replacing any previous one."""
        self.clear(key)
        self._deadlines[key] = deadline
        tick = deadline // self._tick_ms
        if (bucket := self._buckets.get(tick)) is None:
            bucket = self._buckets[tick] = set()
            heappush(self._ticks, tick)
        bucket.add(key)

    def clear(self, key: str) -> bool:
        """Cancel a timer immediately and report whether one existed.

        Cancelling a missing key is harmless. Empty buckets lose their keys at
        once, but their tick IDs can remain until a later sweep or compaction.
        """
        if (deadline := self._deadlines.pop(key, None)) is None:
            return False
        tick = deadline // self._tick_ms
        bucket = self._buckets[tick]
        bucket.discard(key)
        if not bucket:
            del self._buckets[tick]
            self._compact()
        return True

    def has_due(self, now: int) -> bool:
        """Whether due keys or stale tick IDs can consume sweep budget.

        True does not guarantee that the next sweep will return a key.
        """
        return bool(self._ticks) and self._ticks[0] < (now + 1) // self._tick_ms

    def pop_due(self, now: int, limit: int) -> list[str]:
        """Remove due keys, spending one budget unit per key or stale tick ID.

        Nonpositive limits return no keys. Within a bucket, key order is
        unspecified; partially drained buckets remain ready for the next call.
        Compaction can take linear time beyond the sweep budget.
        """
        due: list[str] = []
        while limit > 0 and self.has_due(now):
            tick = self._ticks[0]
            if (bucket := self._buckets.get(tick)) is None:
                heappop(self._ticks)
                limit -= 1
                continue
            while bucket and limit > 0:
                key = bucket.pop()
                del self._deadlines[key]
                due.append(key)
                limit -= 1
            if not bucket:
                heappop(self._ticks)
                del self._buckets[tick]
        self._compact()
        return due

    def _compact(self) -> None:
        if len(self._ticks) > 2 * len(self._buckets) + _COMPACT_SLACK:
            self._ticks = list(self._buckets)
            heapify(self._ticks)
