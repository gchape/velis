import random
import unittest

from tests.helpers import ManualClock
from velis.core.eviction import S3FIFO, EvictionPolicy
from velis.core.store import Store, StoreLimits
from velis.errors import CapacityError


class S3FIFOTests(unittest.TestCase):
    def test_store_defaults_to_s3fifo(self) -> None:
        store = Store(limits=StoreLimits(max_keys=10))
        self.assertIs(StoreLimits().eviction_policy, EvictionPolicy.S3_FIFO)
        self.assertIsInstance(store._eviction, S3FIFO)

    def test_ghost_hits_enter_main_and_explicit_deletion_forgets_the_key(self) -> None:
        store = Store(limits=StoreLimits(max_keys=10))
        policy = store._eviction
        assert isinstance(policy, S3FIFO)
        for index in range(10):
            store.set(str(index), b"v")
        store.set("new", b"v")
        self.assertIn("0", policy._ghost)
        store.set("0", b"again")
        self.assertIn("0", policy._main)
        self.assertNotIn("0", policy._ghost)
        self.assertTrue(store.delete("0"))
        self.assertNotIn("0", policy._main)
        self.assertNotIn("0", policy._ghost)
        store.set("0", b"fresh")
        self.assertIn("0", policy._small)

    def test_repeated_hits_promote_probationary_keys(self) -> None:
        store = Store(limits=StoreLimits(max_keys=10))
        policy = store._eviction
        assert isinstance(policy, S3FIFO)
        for index in range(10):
            store.set(str(index), b"v")
        store.get("0")
        store.get("0")
        store.set("new", b"v")
        self.assertEqual(policy._main["0"], 0)
        self.assertIn("1", policy._ghost)
        self.assertEqual(store.get("0"), b"v")

    def test_hits_and_replacements_increment_capped_counters_without_reordering(
        self,
    ) -> None:
        store = Store(limits=StoreLimits(max_keys=10))
        policy = store._eviction
        assert isinstance(policy, S3FIFO)
        store.set("a", b"first")
        store.set("b", b"second")
        store.set("a", b"updated")
        self.assertEqual(policy._small["a"], 1)
        for _ in range(10):
            store.get("a")
        self.assertEqual(list(policy._small), ["a", "b"])
        self.assertEqual(policy._small["a"], 3)

    def test_nested_eviction_cleans_up_every_victim_and_preserves_ghost_history(
        self,
    ) -> None:
        clock = ManualClock()
        store = Store(clock, limits=StoreLimits(max_keys=10))
        policy = store._eviction
        assert isinstance(policy, S3FIFO)
        for index in range(10):
            key = str(index)
            store.set(key, b"v")
            store.expire(key, 100)
            if index < 9:
                store.get(key)
                store.get(key)
        store.set("new", b"v")
        self.assertEqual(set(store._entries), {str(i) for i in range(1, 9)} | {"new"})
        self.assertEqual(list(policy._ghost), ["9"])
        for key in ("0", "9"):
            self.assertIsNone(store._expiry.deadline(key))
        self.assertEqual(store.stored_bytes, 20)
        clock.advance(100)
        self.assertEqual(store.sweep(), 8)
        self.assertEqual(store.stored_bytes, 4)
        store.set("0", b"fresh")
        self.assertEqual(store.ttl("0"), -1)

    def test_main_queue_reinsertion_decrements_credit(self) -> None:
        policy = S3FIFO(10)
        for key in ("a", "b"):
            policy.access(key)
            self.assertEqual(policy.evict(), [key])
            policy.access(key)
        policy.access("a")
        policy.access("a")
        self.assertEqual(policy.evict(), ["b"])
        self.assertEqual(policy._main["a"], 1)
        self.assertEqual(policy.evict(), ["a"])
        with self.assertRaises(AssertionError):
            policy.evict()

    def test_ttl_deletion_and_variable_sizes_preserve_queue_and_capacity_invariants(
        self,
    ) -> None:
        for capacity in (1, 2, 10, 31):
            clock = ManualClock()
            store = Store(clock, limits=StoreLimits(max_keys=capacity, max_bytes=100))
            policy = store._eviction
            assert isinstance(policy, S3FIFO)
            rng = random.Random(47)
            for step in range(2000):
                key = str(rng.randrange(100))
                with self.subTest(capacity=capacity, step=step):
                    match rng.randrange(7):
                        case 0 | 1:
                            value = rng.randbytes(rng.randrange(120))
                            if len(key.encode()) + len(value) > 100:
                                before = (dict(policy._small), dict(policy._main))
                                with self.assertRaises(CapacityError):
                                    store.set(key, value)
                                self.assertEqual(
                                    (dict(policy._small), dict(policy._main)), before
                                )
                            else:
                                store.set(key, value)
                        case 2:
                            store.get(key)
                        case 3:
                            store.delete(key)
                        case 4:
                            store.expire(key, rng.randrange(5))
                        case 5:
                            clock.advance(rng.randrange(5))
                            store.sweep(limit=rng.randrange(3))
                        case 6:
                            store.persist(key)
                    resident = set(store._entries)
                    self.assertEqual(resident, set(policy._small) | set(policy._main))
                    self.assertFalse(set(policy._small) & set(policy._main))
                    self.assertFalse(resident & set(policy._ghost))
                    self.assertLessEqual(len(policy._ghost), policy._main_limit)
                    self.assertTrue(
                        all(
                            0 <= f <= 3
                            for f in (*policy._small.values(), *policy._main.values())
                        )
                    )
                    self.assertLessEqual(len(store), capacity)
                    self.assertLessEqual(store.stored_bytes, 100)
                    self.assertEqual(
                        store.stored_bytes,
                        sum(
                            len(k.encode()) + len(e.value)
                            for k, e in store._entries.items()
                        ),
                    )
                    self.assertLessEqual(set(store._expiry._entries), resident)
