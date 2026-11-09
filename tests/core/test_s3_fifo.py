import unittest

from tests.helpers import ManualClock
from velis.core.eviction import S3FIFO, EvictionPolicy
from velis.core.store import Store, StoreLimits, TtlStatus


class S3FIFOTests(unittest.TestCase):
    def test_invalid_ratios_are_rejected(self) -> None:
        for ratio in (-1, 0, 1, 2, float("nan")):
            with self.subTest(small_ratio=ratio), self.assertRaises(ValueError):
                S3FIFO(small_ratio=ratio)
        for ratio in (-1, 0):
            with self.subTest(ghost_ratio=ratio), self.assertRaises(ValueError):
                S3FIFO(ghost_ratio=ratio)

    def test_byte_pressure_uses_resident_count_for_queue_targets(self) -> None:
        for max_keys in (10, 100_000):
            with self.subTest(max_keys=max_keys):
                store = Store(limits=StoreLimits(max_keys=max_keys, max_bytes=20))
                for index in range(10):
                    key = str(index)
                    store.set(key, b"v")
                    if index < 9:
                        store.get(key)
                        store.get(key)
                store.set("x", b"v")
                self.assertIs(store.ttl("0"), TtlStatus.MISSING)
                for index in range(1, 10):
                    self.assertEqual(store.get(str(index)), b"v")
                self.assertEqual(store.stats().evictions, 1)

    def test_store_defaults_to_s3fifo(self) -> None:
        store = Store(limits=StoreLimits(max_keys=10))
        self.assertIs(StoreLimits().eviction_policy, EvictionPolicy.S3_FIFO)
        self.assertIsInstance(store._inner._inner._policy, S3FIFO)

    def test_ghost_hits_enter_main_and_explicit_deletion_forgets_the_key(self) -> None:
        store = Store(limits=StoreLimits(max_keys=10))
        policy = store._inner._inner._policy
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
        policy = store._inner._inner._policy
        assert isinstance(policy, S3FIFO)
        for index in range(10):
            store.set(str(index), b"v")
        store.get("0")
        store.get("0")
        store.set("new", b"v")
        self.assertEqual(policy._main["0"], 0)
        self.assertIn("1", policy._ghost)
        self.assertEqual(store.get("0"), b"v")

    def test_replacement_restarts_credit_and_hits_do_not_reorder(
        self,
    ) -> None:
        store = Store(limits=StoreLimits(max_keys=10))
        policy = store._inner._inner._policy
        assert isinstance(policy, S3FIFO)
        store.set("a", b"first")
        store.set("b", b"second")
        store.set("a", b"updated")
        self.assertEqual(policy._small["a"], 0)
        for _ in range(10):
            store.get("a")
        self.assertEqual(list(policy._small), ["b", "a"])
        self.assertEqual(policy._small["a"], 3)

    def test_promotion_that_overflows_main_evicts_exactly_one_key(self) -> None:
        """A promoted key fills main and evicts there, preserving small's tail.

        Ghost re-entry fills main; two later hits qualify a for promotion
        while b remains unreused. Targets follow the current resident count.
        """
        policy = S3FIFO()
        for index in range(9):
            key = f"m{index}"
            policy.access(key)
            self.assertEqual(policy.evict(), key)
            policy.access(key)
        self.assertEqual(len(policy._main), 9)
        for _ in range(3):
            policy.access("a")
        policy.access("b")
        self.assertEqual(policy.evict(), "m0")
        self.assertIn("a", policy._main)
        self.assertEqual(list(policy._small), ["b"])
        self.assertNotIn("b", policy._ghost)

    def test_promotion_eviction_cleans_up_the_victim_everywhere(self) -> None:
        """Main victims leave no ghost and lose their values, bytes and timers."""
        clock = ManualClock()
        store = Store(clock, limits=StoreLimits(max_keys=10))
        policy = store._inner._inner._policy
        assert isinstance(policy, S3FIFO)
        for index in range(10):
            key = str(index)
            store.set(key, b"v")
            store.expire(key, 100)
            if index < 9:
                store.get(key)
                store.get(key)
        store.set("new", b"v")
        self.assertEqual(len(store), 10)
        self.assertIs(store.ttl("0"), TtlStatus.MISSING)
        self.assertTrue(all(str(i) in policy for i in range(1, 10)))
        self.assertIn("new", policy)
        self.assertEqual(list(policy._ghost), [])
        self.assertNotIn("0", policy._main)
        self.assertIsNone(store._inner._expiry.deadline("0"))
        self.assertIsNotNone(store._inner._expiry.deadline("9"))
        self.assertEqual(store.stored_bytes, 22)
        clock.advance(100)
        self.assertEqual(store.sweep(), 9)
        self.assertEqual(store.stored_bytes, 4)
        store.set("0", b"fresh")
        self.assertIs(store.ttl("0"), TtlStatus.PERSISTENT)

    def test_main_queue_reinsertion_decrements_credit(self) -> None:
        policy = S3FIFO()
        for key in ("a", "b"):
            policy.access(key)
            self.assertEqual(policy.evict(), key)
            policy.access(key)
        policy.access("a")
        policy.access("a")
        self.assertEqual(policy.evict(), "b")
        self.assertEqual(policy._main["a"], 1)
        self.assertEqual(policy.evict(), "a")
        with self.assertRaises(AssertionError):
            policy.evict()
