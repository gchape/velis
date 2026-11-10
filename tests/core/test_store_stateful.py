"""Compare bounded LRU operations with an independent value/deadline model."""

from collections import OrderedDict

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from tests.helpers import ManualClock
from velis.config import StoreLimits
from velis.core.eviction import EvictionPolicy
from velis.core.store import Store, StoreStats, TtlStatus
from velis.errors import CapacityError

keys = st.sampled_from(("a", "b", "é", "😀", "long-key"))


class StoreMachine(RuleBasedStateMachine):
    @initialize(max_keys=st.integers(1, 5), max_bytes=st.integers(4, 30))
    def start(self, max_keys: int, max_bytes: int) -> None:
        self.clock = ManualClock()
        self.limits = StoreLimits(
            max_keys=max_keys, max_bytes=max_bytes, eviction_policy=EvictionPolicy.LRU
        )
        self.store = Store(self.clock, limits=self.limits)
        self.model: OrderedDict[str, tuple[bytes, int | None]] = OrderedDict()
        self.hits = self.misses = self.evictions = self.expirations = 0

    def purge(self, key: str) -> None:
        if (entry := self.model.get(key)) is not None:
            deadline = entry[1]
            if deadline is not None and deadline <= self.clock.time_ms:
                del self.model[key]
                self.expirations += 1

    def size(self) -> int:
        return sum(
            len(key.encode()) + len(value) for key, (value, _) in self.model.items()
        )

    def full(self, size: int) -> bool:
        return (
            len(self.model) >= self.limits.max_keys
            or self.size() + size > self.limits.max_bytes
        )

    @rule(key=keys, value=st.binary(max_size=35))
    def write(self, key: str, value: bytes) -> None:
        size = len(key.encode()) + len(value)
        if size > self.limits.max_bytes:
            try:
                self.store.set(key, value)
            except CapacityError:
                return
            raise AssertionError("oversized value accepted")
        self.purge(key)
        self.model.pop(key, None)
        if self.full(size):
            for candidate in tuple(self.model):
                self.purge(candidate)
        while self.full(size):
            self.model.popitem(last=False)
            self.evictions += 1
        self.model[key] = value, None
        self.store.set(key, value)

    @rule(key=keys)
    def read(self, key: str) -> None:
        self.purge(key)
        entry = self.model.get(key)
        assert self.store.get(key) == (None if entry is None else entry[0])
        if entry is None:
            self.misses += 1
        else:
            self.hits += 1
            self.model.move_to_end(key)

    @rule(key=keys)
    def delete(self, key: str) -> None:
        self.purge(key)
        assert self.store.delete(key) == (self.model.pop(key, None) is not None)

    @rule(key=keys, ms=st.integers(0, 10))
    def expire(self, key: str, ms: int) -> None:
        self.purge(key)
        assert self.store.expire(key, ms) == (key in self.model)
        if key in self.model:
            self.model[key] = self.model[key][0], self.clock.time_ms + ms

    @rule(key=keys)
    def persist(self, key: str) -> None:
        self.purge(key)
        entry = self.model.get(key)
        assert self.store.persist(key) == (entry is not None and entry[1] is not None)
        if entry is not None:
            self.model[key] = entry[0], None

    @rule(key=keys)
    def ttl(self, key: str) -> None:
        self.purge(key)
        entry = self.model.get(key)
        if entry is None:
            expected = TtlStatus.MISSING
        elif entry[1] is None:
            expected = TtlStatus.PERSISTENT
        else:
            expected = entry[1] - self.clock.time_ms
        assert self.store.ttl(key) == expected

    @rule(ms=st.integers(0, 10))
    def advance(self, ms: int) -> None:
        self.clock.advance(ms)

    @rule(limit=st.integers(0, 5))
    def sweep(self, limit: int) -> None:
        due = sorted(
            (deadline, key)
            for key, (_, deadline) in self.model.items()
            if deadline is not None and deadline <= self.clock.time_ms
        )[:limit]
        assert self.store.sweep(limit) == len(due)
        for _, key in due:
            del self.model[key]
        self.expirations += len(due)

    @invariant()
    def budgets_timers_and_statistics_agree(self) -> None:
        assert len(self.store) == len(self.model) <= self.limits.max_keys
        assert self.store.stored_bytes == self.size() <= self.limits.max_bytes
        assert self.store.stats() == StoreStats(
            self.hits, self.misses, self.evictions, self.expirations
        )
        assert self.store.has_due_expirations == any(
            deadline is not None and deadline <= self.clock.time_ms
            for _, deadline in self.model.values()
        )


TestStoreMachine = StoreMachine.TestCase
TestStoreMachine.settings = settings(
    max_examples=100, stateful_step_count=80, deadline=None
)
