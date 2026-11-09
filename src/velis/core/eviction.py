"""Eviction policies and key ordering; Store owns values and capacity."""

from collections import OrderedDict
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, assert_never

from velis.validation import require_positive

_EMPTY = "cannot evict from an empty store"


class Eviction(Protocol):
    """Track resident accesses; evict removes metadata and returns victim keys."""

    def access(self, key: str) -> None: ...
    def discard(self, key: str) -> None: ...
    def evict(self) -> list[str]: ...


class LRU:
    """Promote accesses to the newest end and evict the oldest key."""

    def __init__(self) -> None:
        self._order: OrderedDict[str, None] = OrderedDict()

    def discard(self, key: str) -> None:
        self._order.pop(key, None)

    def victim(self) -> str:
        assert self._order, _EMPTY
        return next(iter(self._order))

    def evict(self) -> list[str]:
        key = self.victim()
        self.discard(key)
        return [key]

    def access(self, key: str) -> None:
        self._order[key] = None
        self._order.move_to_end(key)


@dataclass(slots=True, eq=False)
class _FrequencyBucket:
    frequency: int
    keys: OrderedDict[str, None] = field(default_factory=OrderedDict)
    previous: _FrequencyBucket | None = None
    following: _FrequencyBucket | None = None


class LFU:
    """Use linked frequency buckets for constant-time access and eviction.

    The head is always the lowest frequency, even after arbitrary deletions.
    Within a frequency, insertion order breaks ties by least recent access.
    """

    def __init__(self) -> None:
        self._entries: dict[str, _FrequencyBucket] = {}
        self._head: _FrequencyBucket | None = None

    def _insert_after(
        self, previous: _FrequencyBucket | None, frequency: int
    ) -> _FrequencyBucket:
        following = self._head if previous is None else previous.following
        bucket = _FrequencyBucket(frequency, previous=previous, following=following)
        if previous is None:
            self._head = bucket
        else:
            previous.following = bucket
        if following is not None:
            following.previous = bucket
        return bucket

    def _unlink(self, bucket: _FrequencyBucket) -> None:
        if bucket.previous is None:
            self._head = bucket.following
        else:
            bucket.previous.following = bucket.following
        if bucket.following is not None:
            bucket.following.previous = bucket.previous

    def access(self, key: str) -> None:
        current = self._entries.get(key)
        if current is None:
            target = self._head
            if target is None or target.frequency != 1:
                target = self._insert_after(None, 1)
        else:
            target = current.following
            if target is None or target.frequency != current.frequency + 1:
                target = self._insert_after(current, current.frequency + 1)
            del current.keys[key]
            if not current.keys:
                self._unlink(current)
        target.keys[key] = None
        self._entries[key] = target

    def discard(self, key: str) -> None:
        if (bucket := self._entries.pop(key, None)) is not None:
            del bucket.keys[key]
            if not bucket.keys:
                self._unlink(bucket)

    def victim(self) -> str:
        assert self._head is not None, _EMPTY
        return next(iter(self._head.keys))

    def evict(self) -> list[str]:
        key = self.victim()
        self.discard(key)
        return [key]


class S3FIFO:
    """Filter new keys through a small FIFO before the main FIFO.

    Queue targets use key counts: 10% small, 90% main, rounded to at least
    one key each. Ghost history stores at most the main target's key count.
    Store independently enforces its byte budget. Hits increment counters
    capped at three without changing queue order. One eviction can remove
    multiple keys while promoting reused small-queue entries.
    """

    def __init__(self, capacity: int) -> None:
        require_positive(capacity=capacity)
        self._small_limit = max(1, capacity // 10)
        self._main_limit = max(1, capacity - self._small_limit)
        self._small: OrderedDict[str, int] = OrderedDict()
        self._main: OrderedDict[str, int] = OrderedDict()
        self._ghost: OrderedDict[str, None] = OrderedDict()

    def access(self, key: str) -> None:
        if key in self._small:
            if self._small[key] < 3:
                self._small[key] += 1
        elif key in self._main:
            if self._main[key] < 3:
                self._main[key] += 1
        elif key in self._ghost:
            del self._ghost[key]
            self._main[key] = 0
        else:
            self._small[key] = 0

    def discard(self, key: str) -> None:
        self._small.pop(key, None)
        self._main.pop(key, None)
        self._ghost.pop(key, None)

    def _evict_main(self) -> str:
        while True:
            key, frequency = self._main.popitem(last=False)
            if not frequency:
                return key
            self._main[key] = frequency - 1

    def evict(self) -> list[str]:
        assert self._small or self._main, _EMPTY
        if len(self._small) < self._small_limit and self._main:
            return [self._evict_main()]
        evicted: list[str] = []
        while self._small:
            key, frequency = self._small.popitem(last=False)
            if frequency > 1:
                self._main[key] = 0
                if len(self._main) >= self._main_limit:
                    evicted.append(self._evict_main())
            else:
                self._ghost[key] = None
                if len(self._ghost) > self._main_limit:
                    self._ghost.popitem(last=False)
                evicted.append(key)
                break
        if not evicted:
            evicted.append(self._evict_main())
        return evicted


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
