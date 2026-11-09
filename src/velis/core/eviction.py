"""Eviction policies and key ordering; Store owns values and capacity."""

import random
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from velis.errors import CapacityError

_EMPTY = "cannot evict from an empty store"


class Eviction(Protocol):
    def access(self, key: str) -> None: ...
    def discard(self, key: str) -> None: ...
    def victim(self) -> str: ...


class _KeyOrder:
    def __init__(self) -> None:
        self._order: OrderedDict[str, None] = OrderedDict()

    def discard(self, key: str) -> None:
        self._order.pop(key, None)

    def victim(self) -> str:
        assert self._order, _EMPTY
        return next(iter(self._order))


class LRU(_KeyOrder):
    """Promote accesses to the newest end and evict the oldest key."""

    def access(self, key: str) -> None:
        self._order[key] = None
        self._order.move_to_end(key)


class FIFO(_KeyOrder):
    """Evict the oldest inserted key; hits change nothing."""

    def access(self, key: str) -> None:
        self._order.setdefault(key, None)


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


class RandomEviction:
    """Evict a uniformly random key; O(1) via swap-remove."""

    def __init__(self, seed: int | None = None) -> None:
        self._keys: list[str] = []
        self._index: dict[str, int] = {}
        self._rng = random.Random(seed)

    def access(self, key: str) -> None:
        if key not in self._index:
            self._index[key] = len(self._keys)
            self._keys.append(key)

    def discard(self, key: str) -> None:
        if (index := self._index.pop(key, None)) is None:
            return
        last = self._keys.pop()
        if index < len(self._keys):
            self._keys[index] = last
            self._index[last] = index

    def victim(self) -> str:
        assert self._keys, _EMPTY
        return self._rng.choice(self._keys)


class _Reject:
    """Never evict; refuse a write when Store asks for a victim."""

    def access(self, key: str) -> None: ...
    def discard(self, key: str) -> None: ...

    def victim(self) -> str:
        raise CapacityError("store limit exceeded")


class EvictionPolicy(StrEnum):
    REJECT = "reject"
    LRU = "lru"
    FIFO = "fifo"
    LFU = "lfu"
    RANDOM = "random"

    def create(self) -> Eviction:
        return _POLICIES[self]()


_POLICIES: dict[EvictionPolicy, Callable[[], Eviction]] = {
    EvictionPolicy.REJECT: _Reject,
    EvictionPolicy.LRU: LRU,
    EvictionPolicy.FIFO: FIFO,
    EvictionPolicy.LFU: LFU,
    EvictionPolicy.RANDOM: RandomEviction,
}
DEFAULT_EVICTION_POLICY = EvictionPolicy.LRU
