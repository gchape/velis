import unittest

from velis.config import StoreLimits
from velis.core.store import Store


class FifoPolicy:
    """Minimal Eviction implementation that ignores access recency."""

    def __init__(self) -> None:
        self.keys: list[str] = []

    def __len__(self) -> int:
        return len(self.keys)

    def __contains__(self, key: object) -> bool:
        return key in self.keys

    def access(self, key: str) -> None:
        if key not in self.keys:
            self.keys.append(key)

    def discard(self, key: str) -> None:
        if key in self.keys:
            self.keys.remove(key)

    def evict(self) -> str:
        return self.keys.pop(0)


class InjectedPolicyTests(unittest.TestCase):
    def test_store_evicts_the_injected_policys_victim(self) -> None:
        policy = FifoPolicy()
        store = Store(limits=StoreLimits(max_keys=2), policy=policy)
        store.set("a", b"1")
        store.set("b", b"2")
        store.get("a")
        store.set("c", b"3")
        self.assertIsNone(store.get("a"))
        self.assertEqual(store.get("b"), b"2")
        self.assertEqual(policy.keys, ["b", "c"])
        self.assertEqual(store.stats().evictions, 1)

    def test_store_keeps_the_policy_in_step_with_deletes_and_overwrites(self) -> None:
        policy = FifoPolicy()
        store = Store(policy=policy)
        store.set("a", b"1")
        store.set("b", b"2")
        self.assertTrue(store.delete("a"))
        store.set("b", b"3")
        self.assertEqual(policy.keys, ["b"])
