"""Key ordering only; Store owns values, capacity and removal."""

from collections import OrderedDict
from dataclasses import dataclass
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
        return next(iter(self._order))


@dataclass(slots=True, eq=False)
class _Node:
    key: str
    newer: _Node | None = None
    older: _Node | None = None
    visited: bool = False


class Sieve:
    """Insertion order and a moving hand; hits only mark a visited bit.

    The hand scans from older to newer entries, clearing visited bits until
    it finds an unvisited victim. Store removes that key through discard().
    Algorithm: https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo
    """

    def __init__(self) -> None:
        self._nodes: dict[str, _Node] = {}
        self._head: _Node | None = None
        self._tail: _Node | None = None
        self._hand: _Node | None = None

    def access(self, key: str) -> None:
        if (node := self._nodes.get(key)) is not None:
            node.visited = True
            return
        node = _Node(key, older=self._head)
        if self._head is not None:
            self._head.newer = node
        else:
            self._tail = node
        self._head = self._nodes[key] = node

    def discard(self, key: str) -> None:
        node = self._nodes.pop(key, None)
        if node is None:
            return
        if self._hand is node:
            self._hand = node.newer
        if node.newer is None:
            self._head = node.older
        else:
            node.newer.older = node.older
        if node.older is None:
            self._tail = node.newer
        else:
            node.older.newer = node.newer

    def victim(self) -> str:
        node = self._hand or self._tail
        assert node is not None, "cannot evict from an empty store"
        while node.visited:
            node.visited = False
            node = node.newer or self._tail
            assert node is not None
        self._hand = node.newer
        return node.key
