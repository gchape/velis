"""Exact deadlines in an indexed binary min-heap, with eager cancellation."""

from dataclasses import dataclass


@dataclass(slots=True)
class _Entry:
    key: str
    deadline: int
    index: int


class IndexedBinaryHeap:
    """Keep one timer per key without stale entries or index compaction.

    Deadline lookup takes expected constant time; readiness checks are O(1).
    Insertion, rescheduling and cancellation repair at most one logarithmic
    heap path. Equal deadlines have no guaranteed key order.
    """

    def __init__(self) -> None:
        self._heap: list[_Entry] = []
        self._entries: dict[str, _Entry] = {}

    def __len__(self) -> int:
        return len(self._entries)

    def deadline(self, key: str) -> int | None:
        """Return the scheduled deadline, or None when no timer exists."""
        entry = self._entries.get(key)
        return None if entry is None else entry.deadline

    def _up(self, entry: _Entry) -> None:
        index = entry.index
        while index:
            parent = (index - 1) // 2
            other = self._heap[parent]
            if other.deadline <= entry.deadline:
                break
            self._heap[index] = other
            other.index = index
            index = parent
        self._heap[index] = entry
        entry.index = index

    def _down(self, entry: _Entry) -> None:
        index, count = entry.index, len(self._heap)
        while (child := index * 2 + 1) < count:
            if child + 1 < count and (
                self._heap[child + 1].deadline < self._heap[child].deadline
            ):
                child += 1
            other = self._heap[child]
            if entry.deadline <= other.deadline:
                break
            self._heap[index] = other
            other.index = index
            index = child
        self._heap[index] = entry
        entry.index = index

    def set(self, key: str, deadline: int) -> None:
        """Insert a timer or replace its deadline in place."""
        if (entry := self._entries.get(key)) is not None:
            previous = entry.deadline
            entry.deadline = deadline
            if deadline < previous:
                self._up(entry)
            elif deadline > previous:
                self._down(entry)
            return
        entry = _Entry(key, deadline, len(self._heap))
        self._entries[key] = entry
        self._heap.append(entry)
        self._up(entry)

    def clear(self, key: str) -> bool:
        """Cancel a timer immediately and return whether it existed."""
        if (entry := self._entries.pop(key, None)) is None:
            return False
        last = self._heap.pop()
        if entry is not last:
            last.index = entry.index
            self._heap[last.index] = last
            if last.index and (
                last.deadline < self._heap[(last.index - 1) // 2].deadline
            ):
                self._up(last)
            else:
                self._down(last)
        return True

    def has_due(self, now: int) -> bool:
        """Whether the earliest deadline has arrived."""
        return bool(self._heap) and self._heap[0].deadline <= now

    def pop_due(self, now: int, limit: int) -> list[str]:
        """Remove at most limit due timers in deadline order."""
        due: list[str] = []
        while len(due) < limit and self.has_due(now):
            key = self._heap[0].key
            self.clear(key)
            due.append(key)
        return due
