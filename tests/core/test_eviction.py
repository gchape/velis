import contextlib
import random
import unittest

from tests.helpers import ManualClock
from velis.config import EVICTION_POLICIES
from velis.core.store import Store
from velis.errors import CapacityError


class EvictionTests(unittest.TestCase):
    def test_standalone_store_defaults_to_lru(self) -> None:
        store = Store(max_keys=2)
        store.set("a", b"first")
        store.set("b", b"second")
        self.assertEqual(store.get("a"), b"first")
        store.set("c", b"third")
        self.assertIsNone(store.get("b"))
        self.assertEqual(store.get("a"), b"first")
        self.assertEqual(store.get("c"), b"third")

    def test_lfu_prioritizes_frequency_over_latest_access(self) -> None:
        store = Store(max_keys=2, eviction_policy="lfu")
        store.set("frequent", b"a")
        store.set("recent", b"b")
        for _ in range(3):
            store.get("frequent")
        store.get("recent")
        store.set("new", b"c")
        self.assertEqual(store.get("frequent"), b"a")
        self.assertIsNone(store.get("recent"))

    def test_lru_and_lfu_keep_accessed_key(self) -> None:
        for policy in ("lru", "lfu"):
            with self.subTest(policy=policy):
                store = Store(max_keys=2, eviction_policy=policy)
                store.set("a", b"a")
                store.set("b", b"b")
                store.get("a")
                store.set("c", b"c")
                self.assertIsNone(store.get("b"))
                self.assertEqual(store.get("a"), b"a")
                self.assertEqual(store.get("c"), b"c")

    def test_fifo_ignores_reads_and_updates_reinsert(self) -> None:
        store = Store(max_keys=2, eviction_policy="fifo")
        store.set("a", b"a")
        store.set("b", b"b")
        store.get("a")
        store.set("c", b"c")
        self.assertIsNone(store.get("a"))
        store.set("b", b"updated")
        store.set("d", b"d")
        self.assertIsNone(store.get("c"))
        self.assertEqual(store.get("b"), b"updated")

    def test_all_policies_handle_byte_limits_and_replacement_victims(self) -> None:
        for policy in EVICTION_POLICIES[1:]:
            with self.subTest(policy=policy):
                store = Store(max_keys=3, max_bytes=8, eviction_policy=policy)
                store.set("a", b"1")
                store.set("b", b"22")
                store.set("a", b"1234567")
                self.assertEqual(store.get("a"), b"1234567")
                self.assertEqual(store.stored_bytes, 8)
                self.assertEqual(len(store), 1)
                store.set("a", b"")
                self.assertEqual(store.stored_bytes, 1)
                store.set("é", b"12345")
                self.assertEqual(store.stored_bytes, 8)

    def test_oversized_write_preserves_values_ttls_and_policy(self) -> None:
        for policy in EVICTION_POLICIES:
            with self.subTest(policy=policy):
                clock = ManualClock()
                store = Store(clock, max_keys=2, max_bytes=4, eviction_policy=policy)
                store.set("a", b"1")
                store.set("b", b"2")
                store.expire("a", 100)
                for key in ("a", "new"):
                    with self.assertRaises(CapacityError):
                        store.set(key, b"oversized")
                self.assertEqual(store.ttl("a"), 100)
                self.assertEqual(store.stored_bytes, 4)
                self.assertEqual(store.get("a"), b"1")
                self.assertEqual(store.get("b"), b"2")

    def test_eviction_discards_ttl_and_stale_heap_cannot_remove_reinserted_key(
        self,
    ) -> None:
        for policy in EVICTION_POLICIES[1:]:
            with self.subTest(policy=policy):
                clock = ManualClock()
                store = Store(clock, max_keys=1, eviction_policy=policy)
                store.set("a", b"old")
                store.expire("a", 5)
                store.set("b", b"other")
                self.assertEqual(store.ttl("a"), -2)
                store.set("a", b"new")
                clock.advance(5)
                store.sweep()
                self.assertEqual(store.get("a"), b"new")
                self.assertEqual(store.ttl("a"), -1)

    def test_expired_entries_are_reclaimed_before_live_eviction(self) -> None:
        clock = ManualClock()
        store = Store(clock, max_keys=2, eviction_policy="lru")
        store.set("live", b"v")
        store.set("expired", b"v")
        store.expire("expired", 0)
        store.set("new", b"v")
        self.assertEqual(store.get("live"), b"v")
        self.assertIsNone(store.get("expired"))

    def test_random_operations_preserve_both_budgets(self) -> None:
        for policy in EVICTION_POLICIES:
            rng = random.Random(47)
            clock = ManualClock()
            store = Store(clock, max_keys=4, max_bytes=24, eviction_policy=policy)
            for _ in range(1000):
                key = rng.choice(["a", "b", "c", "d", "é", "😀"])
                with contextlib.suppress(CapacityError):
                    store.set(key, rng.randbytes(rng.randrange(28)))
                match rng.randrange(6):
                    case 0:
                        store.delete(key)
                    case 1:
                        store.expire(key, rng.randrange(5))
                    case 2:
                        clock.advance(rng.randrange(5))
                        store.sweep(limit=rng.randrange(3))
                    case 3:
                        store.persist(key)
                    case 4:
                        store.get(key)
                self.assertLessEqual(len(store), 4)
                self.assertLessEqual(store.stored_bytes, 24)
                expected = sum(
                    len(k.encode()) + len(v) for k, v in store._values.items()
                )
                self.assertEqual(store.stored_bytes, expected)

    def test_unknown_policy_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "eviction_policy"):
            Store(eviction_policy="unknown")
