"""Eviction policies: they order resident keys; Store owns values and capacity."""

from collections import OrderedDict
from enum import StrEnum
from heapq import heapify, heappop, heappush
from itertools import count
from typing import Protocol, assert_never

from velis.validation import require_positive

_EMPTY = "cannot evict from an empty store"


class Eviction(Protocol):
    """Order resident keys and choose victims.

    ``access`` records a read or write. ``discard`` forgets a key the store
    removed for its own reasons. ``evict`` picks one victim and forgets it too,
    although a policy may keep ghost history about what it just evicted.
    Length and membership describe resident keys only, excluding ghosts.
    Callers must not evict from an empty policy (a checked precondition, not
    an API); discarding a missing key is harmless.
    """

    def __len__(self) -> int: ...
    def __contains__(self, key: object) -> bool: ...
    def access(self, key: str) -> None: ...
    def discard(self, key: str) -> None: ...
    def evict(self) -> str: ...


class LRU:
    """Promote accesses to the newest end and evict the oldest key."""

    def __init__(self) -> None:
        self._order: OrderedDict[str, None] = OrderedDict()

    def __len__(self) -> int:
        return len(self._order)

    def __contains__(self, key: object) -> bool:
        return key in self._order

    def access(self, key: str) -> None:
        self._order[key] = None
        self._order.move_to_end(key)

    def discard(self, key: str) -> None:
        self._order.pop(key, None)

    def evict(self) -> str:
        assert self._order, _EMPTY
        key, _ = self._order.popitem(last=False)
        return key


class LFU:
    """Evict the least frequently used key, oldest access first within a frequency.

    A min-heap of (frequency, tick, key) orders the candidates. Each access
    pushes a new entry and leaves the old one behind; an entry is live only if
    it matches the key's current record, and stale ones are skipped during
    eviction. The heap is rebuilt once stale entries outnumber live ones, so
    memory stays O(n). Access and eviction are amortized O(log n).
    """

    def __init__(self) -> None:
        self._records: dict[str, tuple[int, int]] = {}
        self._heap: list[tuple[int, int, str]] = []
        self._ticks = count()

    def __len__(self) -> int:
        return len(self._records)

    def __contains__(self, key: object) -> bool:
        return key in self._records

    def access(self, key: str) -> None:
        frequency, _ = self._records.get(key, (0, 0))
        record = (frequency + 1, next(self._ticks))
        self._records[key] = record
        heappush(self._heap, (*record, key))
        self._compact()

    def discard(self, key: str) -> None:
        self._records.pop(key, None)
        self._compact()

    def evict(self) -> str:
        assert self._records, _EMPTY
        while True:
            frequency, tick, key = heappop(self._heap)
            if self._records.get(key) == (frequency, tick):
                del self._records[key]
                self._compact()
                return key

    def _compact(self) -> None:
        """Drop stale heap entries once they outnumber live ones."""
        if len(self._heap) > 2 * len(self._records):
            self._heap = [(f, t, k) for k, (f, t) in self._records.items()]
            heapify(self._heap)


_MAX_HITS = 3
_PROMOTE_HITS = 2


class S3FIFO:
    """Filter new keys through a small FIFO before the main FIFO.

    Queue targets are ratios of the resident key count, not fixed sizes, so
    they stay calibrated whether Store evicts on key count or on bytes: small
    gets ``small_ratio`` of resident keys (at least one), main gets the rest.
    Ghost history is bounded by ``ghost_ratio`` of the resident count, with
    room for at least one remembered key.
    Hits increment counters capped at three without changing queue order. Two
    hits after insertion qualify a small-queue key for promotion. A ghost hit
    enters main directly; explicit deletion forgets both residency and ghost
    history. One eviction removes exactly one key, though it may first promote
    several small entries.
    """

    def __init__(self, *, small_ratio: float = 0.1, ghost_ratio: float = 0.9) -> None:
        if not 0 < small_ratio < 1:
            raise ValueError(f"small_ratio must be in (0, 1), got {small_ratio}")
        require_positive(ghost_ratio=ghost_ratio)
        self._small_ratio = small_ratio
        self._ghost_ratio = ghost_ratio
        self._small: OrderedDict[str, int] = OrderedDict()
        self._main: OrderedDict[str, int] = OrderedDict()
        self._ghost: OrderedDict[str, None] = OrderedDict()

    def __len__(self) -> int:
        return len(self._small) + len(self._main)

    def __contains__(self, key: object) -> bool:
        return key in self._small or key in self._main

    @property
    def _small_limit(self) -> int:
        return max(1, int(len(self) * self._small_ratio))

    @property
    def _main_limit(self) -> int:
        return max(1, len(self) - self._small_limit)

    @property
    def _ghost_limit(self) -> int:
        return max(1, int(len(self) * self._ghost_ratio))

    def access(self, key: str) -> None:
        if key in self._small:
            queue = self._small
        elif key in self._main:
            queue = self._main
        elif key in self._ghost:
            del self._ghost[key]
            self._main[key] = 0
            return
        else:
            self._small[key] = 0
            return
        if queue[key] < _MAX_HITS:
            queue[key] += 1

    def discard(self, key: str) -> None:
        for queue in (self._small, self._main, self._ghost):
            queue.pop(key, None)
        self._trim_ghost()

    def evict(self) -> str:
        assert self._small or self._main, _EMPTY
        if self._main and len(self._small) < self._small_limit:
            victim = self._evict_main()
        else:
            victim = self._evict_small()
        self._trim_ghost()
        return victim

    def _evict_small(self) -> str:
        """Promote reused keys and evict one victim from small or main.

        If a promotion fills main, evict there without also removing the next
        small-queue key. Small victims enter ghost history; main victims do not.
        """
        while self._small:
            key, hits = self._small.popitem(last=False)
            if hits < _PROMOTE_HITS:
                self._ghost[key] = None
                return key
            self._main[key] = 0
            if len(self._main) >= self._main_limit:
                return self._evict_main()
        return self._evict_main()

    def _evict_main(self) -> str:
        """Pop the oldest unreferenced key; referenced ones get another lap."""
        while True:
            key, hits = self._main.popitem(last=False)
            if hits == 0:
                return key
            self._main[key] = hits - 1

    def _trim_ghost(self) -> None:
        """Bound history by current residency after either kind of removal."""
        if not self._ghost:
            return
        limit = self._ghost_limit
        while len(self._ghost) > limit:
            self._ghost.popitem(last=False)


class EvictionPolicy(StrEnum):
    """Configured policy names and their independent, empty implementations."""

    LRU = "lru"
    LFU = "lfu"
    S3_FIFO = "s3-fifo"

    def create(self) -> Eviction:
        """Create fresh policy state for one store to own."""
        match self:
            case EvictionPolicy.LRU:
                return LRU()
            case EvictionPolicy.LFU:
                return LFU()
            case EvictionPolicy.S3_FIFO:
                return S3FIFO()
            case _:
                assert_never(self)
