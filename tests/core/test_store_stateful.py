"""Shrinking operation sequences against a value/deadline reference model."""

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from tests.helpers import ManualClock
from velis.core.eviction import EvictionPolicy
from velis.core.store import Store, StoreLimits, TtlStatus
from velis.errors import CapacityError

KEYS = ("a", "b", "long-key", "é", "😀", *(str(i) for i in range(40)))
keys = st.sampled_from(KEYS)


class StoreMachine(RuleBasedStateMachine):
    """Check values, deadlines and bytes independently of victim selection.

    Capacity victims follow each policy's public membership; dedicated policy
    tests check which victim is selected. Operation sequences shrink on failure.
    """

    @initialize(
        policy=st.sampled_from(tuple(EvictionPolicy)),
        capacity=st.sampled_from((1, 2, 10, 31)),
        max_bytes=st.sampled_from((8, 24, 100)),
    )
    def start(self, policy: EvictionPolicy, capacity: int, max_bytes: int) -> None:
        self.clock = ManualClock()
        self.limits = StoreLimits(
            max_keys=capacity, max_bytes=max_bytes, eviction_policy=policy
        )
        self.store = Store(self.clock, limits=self.limits)
        self.policy = self.store._eviction
        self.model: dict[str, tuple[bytes, int | None]] = {}

    def purge(self, key: str) -> None:
        if (entry := self.model.get(key)) is not None:
            _, deadline = entry
            if deadline is not None and deadline <= self.clock.now_ms():
                del self.model[key]

    @rule(key=keys, value=st.binary(max_size=120))
    def set_value(self, key: str, value: bytes) -> None:
        before = self.store.stats()
        if len(key.encode()) + len(value) > self.limits.max_bytes:
            try:
                self.store.set(key, value)
            except CapacityError:
                pass
            else:
                raise AssertionError("oversized entry was accepted")
            assert self.store.stats() == before
            return
        self.store.set(key, value)
        assert key in self.policy
        self.model[key] = value, None
        for candidate in tuple(self.model):
            if candidate not in self.policy:
                del self.model[candidate]

    @rule(key=keys)
    def get_value(self, key: str) -> None:
        self.purge(key)
        expected = self.model[key][0] if key in self.model else None
        assert self.store.get(key) == expected

    @rule(key=keys)
    def delete_value(self, key: str) -> None:
        self.purge(key)
        expected = self.model.pop(key, None) is not None
        assert self.store.delete(key) == expected

    @rule(key=keys, ms=st.integers(min_value=0, max_value=5))
    def expire_value(self, key: str, ms: int) -> None:
        self.purge(key)
        assert self.store.expire(key, ms) == (key in self.model)
        if key in self.model:
            self.model[key] = self.model[key][0], self.clock.now_ms() + ms

    @rule(key=keys)
    def persist_value(self, key: str) -> None:
        self.purge(key)
        entry = self.model.get(key)
        assert self.store.persist(key) == (entry is not None and entry[1] is not None)
        if entry is not None:
            self.model[key] = entry[0], None

    @rule(key=keys)
    def inspect_ttl(self, key: str) -> None:
        self.purge(key)
        if (entry := self.model.get(key)) is None:
            expected = TtlStatus.MISSING
        elif entry[1] is None:
            expected = TtlStatus.PERSISTENT
        else:
            expected = entry[1] - self.clock.now_ms()
        assert self.store.ttl(key) == expected

    @rule(ms=st.integers(min_value=0, max_value=5))
    def advance(self, ms: int) -> None:
        self.clock.advance(ms)

    @rule(limit=st.integers(min_value=0, max_value=5))
    def sweep(self, limit: int) -> None:
        removed = self.store.sweep(limit)
        missing = [key for key in self.model if key not in self.policy]
        assert len(missing) == removed
        assert removed <= limit
        for key in missing:
            deadline = self.model.pop(key)[1]
            assert deadline is not None and deadline <= self.clock.now_ms()

    @invariant()
    def residents_and_budgets_agree(self) -> None:
        assert len(self.policy) == len(self.store) == len(self.model)
        for key in KEYS:
            assert (key in self.policy) == (key in self.model)
        assert len(self.store) <= self.limits.max_keys
        expected_bytes = sum(
            len(key.encode()) + len(value) for key, (value, _) in self.model.items()
        )
        assert self.store.stored_bytes == expected_bytes <= self.limits.max_bytes


TestStoreMachine = StoreMachine.TestCase
TestStoreMachine.settings = settings(
    max_examples=100, stateful_step_count=100, deadline=None
)
