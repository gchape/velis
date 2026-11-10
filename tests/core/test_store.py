import unittest
from typing import cast
from unittest.mock import Mock, patch

from tests.helpers import ManualClock
from velis.config import StoreLimits
from velis.core.eviction import EvictionPolicy
from velis.core.store import MAX_TTL_MS, SWEEP_LIMIT, Store, StoreStats, TtlStatus
from velis.errors import CapacityError, CommandError


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock(100)
        self.store = Store(self.clock)

    def test_missing_operations_and_stats(self) -> None:
        self.assertIsNone(self.store.get("missing"))
        self.assertFalse(self.store.delete("missing"))
        self.assertFalse(self.store.expire("missing", 10))
        self.assertFalse(self.store.persist("missing"))
        self.assertIs(self.store.ttl("missing"), TtlStatus.MISSING)
        self.assertEqual(self.store.stats(), StoreStats(misses=1))
        self.assertEqual(len(self.store), 0)
        self.assertEqual(self.store.stored_bytes, 0)

    def test_binary_values_and_utf8_byte_accounting(self) -> None:
        for policy in EvictionPolicy:
            with self.subTest(policy=policy):
                store = Store(self.clock, limits=StoreLimits(eviction_policy=policy))
                store.set("😀", b"\x00\xff")
                store.set("empty", b"")
                self.assertEqual(store.get("😀"), b"\x00\xff")
                self.assertEqual(store.get("empty"), b"")
                self.assertEqual(store.stored_bytes, 11)
                store.set("😀", b"x")
                self.assertEqual(store.stored_bytes, 10)
                self.assertTrue(store.delete("😀"))
                self.assertFalse(store.delete("😀"))
                self.assertEqual(store.stored_bytes, 5)
                self.assertEqual(store.stats(), StoreStats(hits=2))

    def test_removing_an_entry_does_not_encode_its_key_again(self) -> None:
        encodings: list[str] = []

        class TrackedKey(str):
            def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
                encodings.append(self)
                return super().encode(encoding, errors)

        key = TrackedKey("😀")
        self.store.set(key, b"value")
        self.assertEqual(self.store.stored_bytes, 9)
        self.assertTrue(self.store.delete(key))
        self.assertEqual(encodings, [key])
        self.assertEqual(self.store.stored_bytes, 0)

    def test_ttl_boundary_is_expired_at_the_deadline(self) -> None:
        self.store.set("k", b"v")
        self.assertIs(self.store.ttl("k"), TtlStatus.PERSISTENT)
        self.assertTrue(self.store.expire("k", 10))
        self.clock.advance(9)
        self.assertEqual(self.store.ttl("k"), 1)
        self.assertEqual(self.store.get("k"), b"v")
        self.clock.advance(1)
        self.assertTrue(self.store.has_due_expirations)
        self.assertEqual(len(self.store), 1)
        self.assertEqual(self.store.stored_bytes, 2)
        self.assertIsNone(self.store.get("k"))
        self.assertFalse(self.store.has_due_expirations)
        self.assertEqual(self.store.stored_bytes, 0)
        self.assertEqual(
            self.store.stats(), StoreStats(hits=1, misses=1, expirations=1)
        )

    def test_each_key_operation_lazily_removes_expired_values(self) -> None:
        for operation in ("get", "delete", "ttl", "persist", "expire", "set"):
            with self.subTest(operation=operation):
                store = Store(self.clock)
                store.set("k", b"old")
                store.expire("k", 0)
                args = {"expire": (10,), "set": (b"new",)}.get(operation, ())
                result = getattr(store, operation)("k", *args)
                self.assertEqual(store.stats().expirations, 1)
                if operation == "set":
                    self.assertEqual(store.get("k"), b"new")
                    self.assertIs(store.ttl("k"), TtlStatus.PERSISTENT)
                else:
                    expected = {"ttl": TtlStatus.MISSING, "get": None}.get(
                        operation, False
                    )
                    self.assertEqual(result, expected)
                    self.assertEqual(len(store), 0)
                self.assertEqual(store.sweep(), 0)

    def test_overwrite_and_persist_cancel_old_deadlines(self) -> None:
        for operation in ("set", "persist"):
            with self.subTest(operation=operation):
                store = Store(self.clock)
                store.set("k", b"value")
                store.expire("k", 5)
                if operation == "set":
                    store.set("k", b"value")
                else:
                    self.assertTrue(store.persist("k"))
                    self.assertFalse(store.persist("k"))
                self.clock.advance(5)
                self.assertFalse(store.has_due_expirations)
                self.assertEqual(store.sweep(), 0)
                self.assertEqual(store.get("k"), b"value")
                self.assertIs(store.ttl("k"), TtlStatus.PERSISTENT)

    def test_reschedule_does_not_expire_at_old_deadline(self) -> None:
        self.store.set("k", b"v")
        self.store.expire("k", 5)
        self.store.expire("k", 10)
        self.clock.advance(5)
        self.assertEqual(self.store.sweep(), 0)
        self.assertEqual(self.store.ttl("k"), 5)
        self.clock.advance(5)
        self.assertEqual(self.store.sweep(), 1)

    def test_invalid_ttl_preserves_existing_deadline_even_when_due(self) -> None:
        self.store.set("k", b"v")
        self.store.expire("k", 10)
        self.clock.advance(10)
        for ms in (-1, MAX_TTL_MS + 1):
            for key in ("k", "missing"):
                with self.subTest(ms=ms, key=key):
                    with self.assertRaisesRegex(CommandError, "ttl out of range"):
                        self.store.expire(key, ms)
                    self.assertEqual(len(self.store), 1)
                    self.assertEqual(self.store.stats(), StoreStats())
        self.assertEqual(self.store.sweep(), 1)

    def test_noninteger_ttl_preserves_value_and_deadline(self) -> None:
        self.store.set("key", b"value")
        self.store.expire("key", 10)
        for value in (1.5, True, "2", float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(CommandError):
                self.store.expire("key", cast(int, value))
            self.assertEqual(self.store.get("key"), b"value")
            self.assertEqual(self.store.ttl("key"), 10)

    def test_maximum_ttl_is_accepted(self) -> None:
        self.store.set("k", b"v")
        self.assertTrue(self.store.expire("k", MAX_TTL_MS))
        self.assertEqual(self.store.ttl("k"), MAX_TTL_MS)

    def test_sweep_is_bounded_and_counts_physical_removals_once(self) -> None:
        for i in range(SWEEP_LIMIT + 2):
            self.store.set(str(i), b"v")
            self.store.expire(str(i), 0)
        self.assertEqual(self.store.sweep(0), 0)
        self.assertEqual(self.store.sweep(), SWEEP_LIMIT)
        self.assertTrue(self.store.has_due_expirations)
        self.assertEqual(self.store.sweep(1), 1)
        self.assertEqual(self.store.sweep(), 1)
        self.assertEqual(self.store.sweep(), 0)
        self.assertFalse(self.store.has_due_expirations)
        self.assertEqual(self.store.stats().expirations, SWEEP_LIMIT + 2)
        self.assertEqual(self.store.stored_bytes, 0)

    def test_operations_sample_the_clock_once(self) -> None:
        clock = Mock()
        clock.now_ms.return_value = 100
        store = Store(clock)
        operations = (
            lambda: store.set("k", b"v"),
            lambda: store.get("k"),
            lambda: store.expire("k", 10),
            lambda: store.ttl("k"),
            lambda: store.persist("k"),
            lambda: store.has_due_expirations,
            lambda: store.sweep(),
            lambda: store.delete("k"),
        )
        for operation in operations:
            clock.reset_mock()
            operation()
            clock.now_ms.assert_called_once_with()

    def test_stats_are_snapshots(self) -> None:
        snapshot = self.store.stats()
        self.store.get("missing")
        self.assertEqual(snapshot, StoreStats())
        self.assertEqual(self.store.stats(), StoreStats(misses=1))


class StoreCapacityTests(unittest.TestCase):
    def test_key_and_byte_limits_hold_for_every_policy(self) -> None:
        for policy in EvictionPolicy:
            for limits in (
                StoreLimits(max_keys=2, eviction_policy=policy),
                StoreLimits(max_bytes=4, eviction_policy=policy),
            ):
                with self.subTest(policy=policy, limits=limits):
                    store = Store(limits=limits)
                    for key in "abc":
                        store.set(key, b"v")
                    self.assertEqual(len(store), 2)
                    self.assertEqual(store.stored_bytes, 4)
                    self.assertEqual(store.get("c"), b"v")
                    self.assertEqual(store.stats().evictions, 1)

    def test_large_write_can_evict_multiple_keys_and_clears_their_timers(self) -> None:
        for policy in EvictionPolicy:
            with self.subTest(policy=policy):
                clock = ManualClock()
                store = Store(
                    clock, limits=StoreLimits(max_bytes=8, eviction_policy=policy)
                )
                for key in "abc":
                    store.set(key, b"v")
                    store.expire(key, 1)
                store.set("d", b"x" * 7)
                self.assertEqual(len(store), 1)
                self.assertEqual(store.stored_bytes, 8)
                self.assertEqual(store.stats().evictions, 3)
                clock.advance(1)
                self.assertEqual(store.sweep(), 0)
                self.assertEqual(store.get("d"), b"x" * 7)

    def test_oversized_overwrite_is_atomic_before_expiry_cleanup(self) -> None:
        clock = ManualClock()
        store = Store(clock, limits=StoreLimits(max_keys=1, max_bytes=4))
        store.set("é", b"ok")
        store.expire("é", 0)
        with (
            patch.object(clock, "now_ms", side_effect=AssertionError("clock read")),
            self.assertRaisesRegex(CapacityError, "byte limit"),
        ):
            store.set("é", b"bad")
        self.assertEqual(len(store), 1)
        self.assertEqual(store.stored_bytes, 4)
        self.assertEqual(store.stats(), StoreStats())
        self.assertEqual(store.sweep(), 1)

    def test_overwrite_reclaims_its_own_bytes_before_checking_capacity(self) -> None:
        for policy in EvictionPolicy:
            with self.subTest(policy=policy):
                store = Store(
                    limits=StoreLimits(max_keys=2, max_bytes=6, eviction_policy=policy)
                )
                store.set("a", b"123")
                store.set("b", b"x")
                store.set("a", b"456")
                self.assertEqual(store.get("b"), b"x")
                self.assertEqual(store.get("a"), b"456")
                self.assertEqual(store.stats().evictions, 0)

    def test_capacity_sweep_reclaims_expired_keys_before_live_victims(self) -> None:
        for policy in EvictionPolicy:
            with self.subTest(policy=policy):
                clock = ManualClock()
                store = Store(
                    clock, limits=StoreLimits(max_keys=2, eviction_policy=policy)
                )
                store.set("live", b"v")
                store.set("expired", b"v")
                store.expire("expired", 0)
                store.set("new", b"v")
                self.assertEqual(store.get("live"), b"v")
                self.assertEqual(store.get("new"), b"v")
                self.assertEqual(store.stats().expirations, 1)
                self.assertEqual(store.stats().evictions, 0)

    def test_only_get_and_set_promote_lru_keys(self) -> None:
        for operation in ("get", "ttl", "expire", "persist"):
            with self.subTest(operation=operation):
                store = Store(
                    limits=StoreLimits(max_keys=2, eviction_policy=EvictionPolicy.LRU)
                )
                store.set("a", b"a")
                store.expire("a", MAX_TTL_MS)
                store.set("b", b"b")
                if operation == "expire":
                    store.expire("a", MAX_TTL_MS)
                else:
                    getattr(store, operation)("a")
                store.set("c", b"c")
                victim = "b" if operation == "get" else "a"
                self.assertIsNone(store.get(victim))

    def test_overwrite_resets_lfu_history(self) -> None:
        store = Store(
            limits=StoreLimits(max_keys=2, eviction_policy=EvictionPolicy.LFU)
        )
        store.set("a", b"a")
        for _ in range(5):
            store.get("a")
        store.set("b", b"b")
        store.get("b")
        store.set("a", b"new")
        store.set("c", b"c")
        self.assertIsNone(store.get("a"))
        self.assertEqual(store.get("b"), b"b")
