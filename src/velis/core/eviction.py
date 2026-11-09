"""Eviction policies: they order resident keys; Store owns values and capacity."""

from collections import OrderedDict
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, assert_never

from velis.validation import require_positive

_EMPTY = "cannot evict from an empty store"


class Eviction(Protocol):
    """Order resident keys and choose victims.

    ``access`` records a read or write. ``discard`` forgets a key the store
    removed for its own reasons. ``evict`` picks one victim and forgets it too,
    although a policy may keep ghost history about what it just evicted.
    Length and membership describe resident keys only, excluding ghosts.
    Evicting from an empty policy raises AssertionError; discarding a missing
    key is harmless.
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


@dataclass(slots=True, eq=False)
class _Bucket:
    """Keys sharing one access frequency, linked in a circular list."""

    frequency: int
    keys: OrderedDict[str, None] = field(
        default_factory=OrderedDict[str, None], init=False
    )
    prev: _Bucket = field(init=False, repr=False)
    next: _Bucket = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.prev = self.next = self


class LFU:
    """Evict the least frequently used key, oldest first within a frequency.

    Buckets form a circular list around a sentinel of frequency 0, ordered by
    ascending frequency, so the first bucket is always the lowest one and no
    link is ever None. New keys start at frequency one; each later access adds
    one, even when deletion has left gaps between buckets. Access, discard and
    eviction are all O(1).
    """

    def __init__(self) -> None:
        self._root = _Bucket(0)
        self._buckets: dict[str, _Bucket] = {}

    def __len__(self) -> int:
        return len(self._buckets)

    def __contains__(self, key: object) -> bool:
        return key in self._buckets

    def access(self, key: str) -> None:
        current = self._buckets.get(key)
        after, frequency = (
            (self._root, 1) if current is None else (current, current.frequency + 1)
        )
        target = after.next
        if target.frequency != frequency:
            target = self._insert_after(after, frequency)
        target.keys[key] = None
        self._buckets[key] = target
        if current is not None:
            self._release(current, key)

    def discard(self, key: str) -> None:
        if (bucket := self._buckets.pop(key, None)) is not None:
            self._release(bucket, key)

    def evict(self) -> str:
        lowest = self._root.next
        assert lowest is not self._root, _EMPTY
        key = next(iter(lowest.keys))
        self.discard(key)
        return key

    @staticmethod
    def _insert_after(node: _Bucket, frequency: int) -> _Bucket:
        bucket = _Bucket(frequency)
        bucket.prev, bucket.next = node, node.next
        node.next.prev = bucket
        node.next = bucket
        return bucket

    @staticmethod
    def _release(bucket: _Bucket, key: str) -> None:
        """Remove a key from its bucket, unlinking the bucket once empty."""
        del bucket.keys[key]
        if not bucket.keys:
            bucket.prev.next = bucket.next
            bucket.next.prev = bucket.prev


_MAX_HITS = 3
_PROMOTE_HITS = 2


class S3FIFO:
    """Filter new keys through a small FIFO before the main FIFO.

    Queue targets use key counts: 10% small, 90% main, rounded to at least
    one key each. Ghost history stores at most the main target's key count.
    Store independently enforces its byte budget. Hits increment counters
    capped at three without changing queue order. Two hits after insertion
    qualify a small-queue key for promotion. A ghost hit enters main directly;
    explicit deletion forgets both residency and ghost history. One eviction
    removes exactly one key, though it may first promote several small entries.
    """

    def __init__(self, capacity: int) -> None:
        require_positive(capacity=capacity)
        self._small_limit = max(1, capacity // 10)
        self._main_limit = max(1, capacity - self._small_limit)
        self._small: OrderedDict[str, int] = OrderedDict()
        self._main: OrderedDict[str, int] = OrderedDict()
        self._ghost: OrderedDict[str, None] = OrderedDict()

    def __len__(self) -> int:
        return len(self._small) + len(self._main)

    def __contains__(self, key: object) -> bool:
        return key in self._small or key in self._main

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

    def evict(self) -> str:
        assert self._small or self._main, _EMPTY
        if self._main and len(self._small) < self._small_limit:
            return self._evict_main()
        return self._evict_small()

    def _evict_small(self) -> str:
        """Promote reused keys and evict one victim from small or main.

        If a promotion fills main, evict there without also removing the next
        small-queue key. Small victims enter ghost history; main victims do not.
        """
        while self._small:
            key, hits = self._small.popitem(last=False)
            if hits < _PROMOTE_HITS:
                self._remember(key)
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

    def _remember(self, key: str) -> None:
        """Record an evicted key in the bounded ghost history."""
        self._ghost[key] = None
        if len(self._ghost) > self._main_limit:
            self._ghost.popitem(last=False)


class EvictionPolicy(StrEnum):
    LRU = "lru"
    LFU = "lfu"
    S3_FIFO = "s3-fifo"

    def create(self, *, max_keys: int) -> Eviction:
        match self:
            case EvictionPolicy.LRU:
                return LRU()
            case EvictionPolicy.LFU:
                return LFU()
            case EvictionPolicy.S3_FIFO:
                return S3FIFO(max_keys)
            case _:
                assert_never(self)


DEFAULT_EVICTION_POLICY = EvictionPolicy.S3_FIFO
