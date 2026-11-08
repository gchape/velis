"""Key ordering only; Store owns values, capacity and removal."""

import random
from collections import OrderedDict, defaultdict
from typing import Protocol


class Eviction(Protocol):
    def access(self, key: str) -> None: ...

    def discard(self, key: str) -> None: ...

    def victim(self) -> str: ...


class Lru:
    """Promote accesses to the newest end and evict the oldest key."""

    def __init__(self) -> None:
        self._order: OrderedDict[str, None] = OrderedDict()

    def access(self, key: str) -> None:
        self._order[key] = None
        self._order.move_to_end(key)

    def discard(self, key: str) -> None:
        self._order.pop(key, None)

    def victim(self) -> str:
        assert self._order, "cannot evict from an empty store"
        return next(iter(self._order))


class Fifo:
    """Evict the oldest inserted key; hits change nothing."""

    def __init__(self) -> None:
        self._order: OrderedDict[str, None] = OrderedDict()

    def access(self, key: str) -> None:
        self._order.setdefault(key, None)

    def discard(self, key: str) -> None:
        self._order.pop(key, None)

    def victim(self) -> str:
        assert self._order, "cannot evict from an empty store"
        return next(iter(self._order))


class Lfu:
    """Evict the least frequently used key; ties go to the least recently used."""

    def __init__(self) -> None:
        self._freq: dict[str, int] = {}
        self._buckets: defaultdict[int, OrderedDict[str, None]] = defaultdict(
            OrderedDict
        )

    def access(self, key: str) -> None:
        f = self._freq.get(key, 0)
        if f:
            self._remove(key, f)
        self._freq[key] = f + 1
        self._buckets[f + 1][key] = None

    def discard(self, key: str) -> None:
        if (f := self._freq.get(key)) is not None:
            self._remove(key, f)

    def victim(self) -> str:
        assert self._buckets, "cannot evict from an empty store"
        return next(iter(self._buckets[min(self._buckets)]))

    def _remove(self, key: str, f: int) -> None:
        bucket = self._buckets[f]
        del bucket[key]
        if not bucket:
            del self._buckets[f]
        del self._freq[key]


class Random:
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
        if (i := self._index.pop(key, None)) is None:
            return
        last = self._keys.pop()
        if i < len(self._keys):
            self._keys[i] = last
            self._index[last] = i

    def victim(self) -> str:
        assert self._keys, "cannot evict from an empty store"
        return self._rng.choice(self._keys)
