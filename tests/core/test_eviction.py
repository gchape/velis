import contextlib
import random
import unittest
from typing import cast

from tests.helpers import ManualClock
from velis.core.store import EvictionPolicy, Store, StoreLimits
from velis.errors import CapacityError


class EvictionTests(unittest.TestCase):
    def test_standalone_store_defaults_to_lru(self) -> None:
        store = Store(limits=StoreLimits(max_keys=2))
        store.set("a", b"first")
        store.set("b", b"second")
        self.assertEqual(store.get("a"), b"first")
        store.set("c", b"third")
        self.assertIsNone(store.get("b"))
        self.assertEqual(store.get("a"), b"first")
        self.assertEqual(store.get("c"), b"third")

    def test_lru_and_sieve_keep_accessed_key(self) -> None:
        for policy in (EvictionPolicy.LRU, EvictionPolicy.SIEVE):
            with self.subTest(policy=policy):
                store = Store(limits=StoreLimits(max_keys=2, eviction_policy=policy))
                store.set("a", b"a")
                store.set("b", b"b")
                store.get("a")
                store.set("c", b"c")
                self.assertIsNone(store.get("b"))
                self.assertEqual(store.get("a"), b"a")
                self.assertEqual(store.get("c"), b"c")

    def test_sieve_hand_keeps_insertion_order_and_skips_visited_entries(self) -> None:
        store = Store(
            limits=StoreLimits(max_keys=3, eviction_policy=EvictionPolicy.SIEVE)
        )
        for key in ("a", "b", "c"):
            store.set(key, key.encode())
        self.assertEqual(store.get("a"), b"a")
        store.set("d", b"d")
        self.assertEqual(set(store._values), {"a", "c", "d"})
        store.set("e", b"e")
        self.assertEqual(set(store._values), {"a", "d", "e"})
        store.set("f", b"f")
        self.assertEqual(set(store._values), {"a", "e", "f"})

    def test_sieve_wraps_when_every_key_was_visited(self) -> None:
        store = Store(
            limits=StoreLimits(max_keys=2, eviction_policy=EvictionPolicy.SIEVE)
        )
        for key in ("a", "b"):
            store.set(key, key.encode())
            store.get(key)
        store.set("c", b"c")
        store.set("d", b"d")
        self.assertEqual(set(store._values), {"c", "d"})

    def test_sieve_updates_count_as_accesses_without_reinserting(self) -> None:
        store = Store(
            limits=StoreLimits(max_keys=2, eviction_policy=EvictionPolicy.SIEVE)
        )
        store.set("a", b"a")
        store.set("b", b"b")
        store.set("a", b"updated")
        store.set("c", b"c")
        self.assertEqual(store.get("a"), b"updated")
        self.assertIsNone(store.get("b"))

    def test_ttl_operations_do_not_promote_eviction_candidates(self) -> None:
        for policy in (EvictionPolicy.LRU, EvictionPolicy.SIEVE):
            with self.subTest(policy=policy):
                store = Store(limits=StoreLimits(max_keys=2, eviction_policy=policy))
                store.set("a", b"a")
                store.set("b", b"b")
                store.expire("a", 10_000)
                store.ttl("a")
                store.persist("a")
                store.set("c", b"c")
                self.assertIsNone(store.get("a"))

    def test_deleting_the_sieve_hand_and_all_entries_allows_reuse(self) -> None:
        for removal in ("delete", "expire"):
            with self.subTest(removal=removal):
                clock = ManualClock()
                store = Store(
                    clock,
                    limits=StoreLimits(
                        max_keys=3, eviction_policy=EvictionPolicy.SIEVE
                    ),
                )
                for key in ("a", "b", "c", "d"):
                    store.set(key, b"x")
                if removal == "delete":
                    store.delete("b")
                else:
                    store.expire("b", 0)
                    store.sweep()
                store.set("e", b"x")
                store.set("f", b"x")
                self.assertEqual(set(store._values), {"d", "e", "f"})
                for key in tuple(store._values):
                    store.delete(key)
                store.set("again", b"ok")
                self.assertEqual(store.get("again"), b"ok")

    def test_all_policies_handle_byte_limits_and_replacement_victims(self) -> None:
        for policy in tuple(EvictionPolicy)[1:]:
            with self.subTest(policy=policy):
                store = Store(
                    limits=StoreLimits(max_keys=3, max_bytes=8, eviction_policy=policy)
                )
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
        for policy in EvictionPolicy:
            with self.subTest(policy=policy):
                clock = ManualClock()
                store = Store(
                    clock,
                    limits=StoreLimits(max_keys=2, max_bytes=4, eviction_policy=policy),
                )
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

    def test_eviction_cancels_ttl_before_a_key_is_reinserted(
        self,
    ) -> None:
        for policy in tuple(EvictionPolicy)[1:]:
            with self.subTest(policy=policy):
                clock = ManualClock()
                store = Store(
                    clock, limits=StoreLimits(max_keys=1, eviction_policy=policy)
                )
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
        store = Store(
            clock, limits=StoreLimits(max_keys=2, eviction_policy=EvictionPolicy.LRU)
        )
        store.set("live", b"v")
        store.set("expired", b"v")
        store.expire("expired", 0)
        store.set("new", b"v")
        self.assertEqual(store.get("live"), b"v")
        self.assertIsNone(store.get("expired"))

    def test_random_operations_preserve_both_budgets(self) -> None:
        for policy in EvictionPolicy:
            rng = random.Random(47)
            clock = ManualClock()
            store = Store(
                clock,
                limits=StoreLimits(max_keys=4, max_bytes=24, eviction_policy=policy),
            )
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
            Store(limits=StoreLimits(eviction_policy=cast(EvictionPolicy, "unknown")))
