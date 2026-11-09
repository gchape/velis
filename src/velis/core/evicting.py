"""Capacity layer: bounds keys and bytes by asking a policy for victims."""

from collections.abc import Callable

from velis.config import StoreLimits
from velis.core.kv import KV, entry_size
from velis.errors import CapacityError


class Evicting:
    """Wrap a KV, keeping its eviction policy in lockstep with its contents.

    Every key leaves through exactly one of two paths: ``delete`` (the caller
    removed it, so the policy discards it and its ghost history) or capacity
    eviction (the policy chose it and already forgot it). An overwrite is a
    delete followed by a fresh insert, so it restarts its history and can
    never select itself as a victim.

    Two optional hooks let a layer above react:
    ``on_evict(key)`` runs after each capacity victim is removed, and
    ``reclaim()`` runs once when room is needed, before any live key is evicted.
    """

    def __init__(self, inner: KV, limits: StoreLimits) -> None:
        self._inner = inner
        self._limits = limits
        self._policy = limits.eviction_policy.create()
        self.evictions = 0
        self.on_evict: Callable[[str], object] = lambda key: None
        self.reclaim: Callable[[], object] = lambda: None

    def get(self, key: str) -> bytes | None:
        value = self._inner.get(key)
        if value is not None:
            self._policy.access(key)
        return value

    def set(self, key: str, value: bytes) -> None:
        """Reject oversized entries before changing anything, then make room."""
        size = entry_size(key, value)
        if size > self._limits.max_bytes:
            raise CapacityError("store byte limit exceeded")
        if key in self._inner:
            self.delete(key)
        if self._full(size):
            self.reclaim()
        while self._full(size):
            victim = self._policy.evict()
            self._inner.delete(victim)
            self.evictions += 1
            self.on_evict(victim)
        self._inner.set(key, value)
        self._policy.access(key)

    def delete(self, key: str) -> bool:
        self._policy.discard(key)
        return self._inner.delete(key)

    def __contains__(self, key: object) -> bool:
        return key in self._inner

    def __len__(self) -> int:
        return len(self._inner)

    @property
    def stored_bytes(self) -> int:
        return self._inner.stored_bytes

    def _full(self, size: int) -> bool:
        """Whether adding one non-resident entry of this size breaks a limit."""
        return (
            len(self._inner) >= self._limits.max_keys
            or self._inner.stored_bytes + size > self._limits.max_bytes
        )
