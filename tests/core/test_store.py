import unittest

from tests.helpers import ManualClock
from velis.core.eviction import EvictionPolicy
from velis.core.store import MAX_TTL_MS, Store, StoreLimits
from velis.errors import CapacityError, CommandError


class StoreTests(unittest.TestCase):
    def test_delete_releases_capacity_and_removes_deadline(self) -> None:
        store = Store(self.clock, limits=StoreLimits(max_keys=1, max_bytes=4))
        store.set("é", b"12")
        store.expire("é", 10)
        self.assertTrue(store.delete("é"))
        self.assertEqual(store.stored_bytes, 0)
        self.assertFalse(store.delete("é"))
        store.set("é", b"")
        self.clock.advance(10)
        store.sweep()
        self.assertEqual(store.get("é"), b"")
        self.assertTrue(store.delete("é"))
        self.assertEqual(store.stored_bytes, 0)

    def test_ttl_boundaries_and_lazy_cleanup(self) -> None:
        self.assertEqual(self.store.ttl("k"), -2)
        self.store.set("k", b"v")
        self.assertEqual(self.store.ttl("k"), -1)
        self.store.expire("k", MAX_TTL_MS)
        self.assertEqual(self.store.ttl("k"), MAX_TTL_MS)
        self.clock.advance(MAX_TTL_MS - 1)
        self.assertEqual(self.store.ttl("k"), 1)
        self.clock.advance(1)
        self.assertEqual(self.store.ttl("k"), -2)
        self.assertEqual(self.store.stored_bytes, 0)

    def test_persist_preserves_value_and_cancels_expiry(self) -> None:
        self.assertFalse(self.store.persist("k"))
        self.store.set("k", b"v")
        self.assertFalse(self.store.persist("k"))
        self.store.expire("k", 5)
        size = self.store.stored_bytes
        self.assertTrue(self.store.persist("k"))
        self.assertFalse(self.store.persist("k"))
        self.clock.advance(5)
        self.store.sweep()
        self.assertEqual(self.store.get("k"), b"v")
        self.assertEqual(self.store.stored_bytes, size)
        self.assertEqual(self.store.ttl("k"), -1)

    def test_expired_keys_cannot_be_deleted_or_persisted_as_live(self) -> None:
        for operation in (self.store.delete, self.store.persist):
            self.store.set("k", b"v")
            self.store.expire("k", 0)
            self.assertFalse(operation("k"))
            self.assertEqual(self.store.ttl("k"), -2)
            self.assertEqual(self.store.stored_bytes, 0)

    def test_key_limit_allows_replacement_and_reclaims_expired_capacity(self) -> None:
        store = Store(
            self.clock,
            limits=StoreLimits(max_keys=1, eviction_policy=EvictionPolicy.REJECT),
        )
        store.set("a", b"old")
        with self.assertRaisesRegex(CapacityError, "store limit"):
            store.set("b", b"new")
        self.assertEqual(store.get("a"), b"old")
        store.set("a", b"replacement")
        store.expire("a", 1)
        self.clock.advance(1)
        store.sweep()
        self.assertEqual(store.stored_bytes, 0)
        store.set("b", b"new")
        self.assertEqual(store.get("b"), b"new")

    def test_byte_limit_counts_utf8_keys_and_replacement_deltas(self) -> None:
        store = Store(
            self.clock,
            limits=StoreLimits(max_bytes=6, eviction_policy=EvictionPolicy.REJECT),
        )
        store.set("é", b"1234")
        self.assertEqual(store.stored_bytes, 6)
        with self.assertRaisesRegex(CapacityError, "store limit"):
            store.set("x", b"")
        store.set("é", b"1")
        store.set("x", b"12")
        self.assertEqual(store.stored_bytes, 6)
        store.expire("é", 0)
        self.assertIsNone(store.get("é"))
        self.assertEqual(store.stored_bytes, 3)
        store.set("x", b"")
        self.assertEqual(store.stored_bytes, 1)
        store.expire("x", 0)
        store.sweep()
        self.assertEqual(store.stored_bytes, 0)

    def test_rejected_replacement_preserves_value_and_deadline(self) -> None:
        store = Store(self.clock, limits=StoreLimits(max_bytes=4))
        store.set("k", b"old")
        store.expire("k", 100)
        with self.assertRaises(CapacityError):
            store.set("k", b"oversized")
        self.assertEqual(store.stored_bytes, 4)
        self.assertEqual(store.get("k"), b"old")
        self.clock.advance(100)
        self.assertIsNone(store.get("k"))
        self.assertEqual(store.stored_bytes, 0)

    def test_store_limits_must_be_positive(self) -> None:
        for keys, size in ((0, 1), (1, 0), (1, -1)):
            with self.subTest(keys=keys, size=size), self.assertRaises(ValueError):
                Store(limits=StoreLimits(max_keys=keys, max_bytes=size))

    def test_validated_limits_can_be_shared_without_unpacking(self) -> None:
        limits = StoreLimits(
            max_keys=1, max_bytes=4, eviction_policy=EvictionPolicy.REJECT
        )
        store = Store(self.clock, limits=limits)
        store.set("k", b"old")
        with self.assertRaises(CapacityError):
            store.set("x", b"new")

    def test_reject_reclaims_expired_entries_before_refusing_a_write(self) -> None:
        store = Store(
            self.clock,
            limits=StoreLimits(
                max_keys=1, max_bytes=4, eviction_policy=EvictionPolicy.REJECT
            ),
        )
        store.set("old", b"v")
        store.expire("old", 1)
        self.clock.advance(1)
        store.set("new", b"v")
        self.assertIsNone(store.get("old"))
        self.assertEqual(store.get("new"), b"v")
        self.assertEqual(store.stored_bytes, 4)

    def setUp(self) -> None:
        self.clock = ManualClock(1_000)
        self.store = Store(self.clock)

    def test_get_set(self) -> None:
        self.store.set("k", b"v")
        self.assertEqual(self.store.get("k"), b"v")
        self.assertIsNone(self.store.get("missing"))

    def test_lazy_expiry(self) -> None:
        self.store.set("k", b"v")
        self.assertTrue(self.store.expire("k", 500))
        self.clock.advance(499)
        self.assertEqual(self.store.get("k"), b"v")
        self.clock.advance(1)
        self.assertIsNone(self.store.get("k"))

    def test_set_clears_ttl(self) -> None:
        self.store.set("k", b"v")
        self.store.expire("k", 500)
        self.store.set("k", b"w")
        self.clock.advance(10_000)
        self.assertEqual(self.store.get("k"), b"w")

    def test_expire_missing_key(self) -> None:
        self.assertFalse(self.store.expire("nope", 5_000))

    def test_sweep_frees_keys_nobody_reads(self) -> None:
        """Only due keys are removed, even when nobody reads them."""
        for i in range(5):
            self.store.set(f"k{i}", b"v")
            self.store.expire(f"k{i}", 100)
        self.assertEqual(self.store.sweep(), 0)
        self.clock.advance(200)
        self.assertEqual(self.store.sweep(), 5)
        self.assertEqual(len(self.store), 0)

    def test_sweep_ignores_canceled_timers(self) -> None:
        """Overwriting a value unlinks its old timer."""
        self.store.set("k", b"v")
        self.store.expire("k", 100)
        self.store.set("k", b"w")
        self.clock.advance(500)
        self.assertEqual(self.store.sweep(), 0)
        self.assertEqual(self.store.get("k"), b"w")

    def test_expire_replaces_previous_deadline(self) -> None:
        self.store.set("k", b"v")
        self.store.expire("k", 100)
        self.clock.advance(50)
        self.assertTrue(self.store.expire("k", 7_950))
        self.clock.advance(100)
        self.assertEqual(self.store.sweep(), 0)
        self.assertEqual(self.store.get("k"), b"v")
        self.clock.advance(7_850)
        self.assertEqual(self.store.sweep(), 1)
        self.assertIsNone(self.store.get("k"))

    def test_expire_cannot_revive_expired_key(self) -> None:
        self.store.set("k", b"v")
        self.store.expire("k", 100)
        self.clock.advance(100)
        self.assertFalse(self.store.expire("k", 500))
        self.assertIsNone(self.store.get("k"))

    def test_zero_ttl_expires_immediately(self) -> None:
        self.store.set("k", b"v")
        self.assertTrue(self.store.expire("k", 0))
        self.assertIsNone(self.store.get("k"))

    def test_invalid_ttl_preserves_value_and_existing_expiry(self) -> None:
        self.store.set("key", b"value")
        self.store.expire("key", 100)
        for ms in (-1, MAX_TTL_MS + 1):
            with self.subTest(ms=ms):
                with self.assertRaisesRegex(CommandError, "ttl out of range"):
                    self.store.expire("key", ms)
                self.assertEqual(self.store.get("key"), b"value")
        self.clock.advance(100)
        self.assertEqual(self.store.sweep(), 1)
        self.assertIsNone(self.store.get("key"))

    def test_invalid_ttl_is_rejected_for_missing_keys(self) -> None:
        for ms in (-1, MAX_TTL_MS + 1):
            with self.subTest(ms=ms), self.assertRaises(CommandError):
                self.store.expire("missing", ms)

    def test_maximum_ttl_expires_at_its_deadline(self) -> None:
        self.store.set("key", b"value")
        self.assertTrue(self.store.expire("key", MAX_TTL_MS))
        self.clock.advance(MAX_TTL_MS - 1)
        self.assertEqual(self.store.get("key"), b"value")
        self.clock.advance(1)
        self.assertIsNone(self.store.get("key"))

    def test_sweep_respects_limit(self) -> None:
        for i in range(5):
            self.store.set(str(i), b"v")
            self.store.expire(str(i), 100)
        self.clock.advance(100)
        self.assertEqual(self.store.sweep(limit=2), 2)
        self.assertEqual(len(self.store), 3)
        self.assertEqual(self.store.sweep(), 3)

    def test_repeated_ttl_updates_reuse_one_timer_and_preserve_latest_expiry(
        self,
    ) -> None:
        self.store.set("key", b"value")
        for ms in range(10_000, 20_000):
            self.store.expire("key", ms)
        self.assertEqual(len(self.store._expiry), 1)
        self.clock.advance(19_998)
        self.assertEqual(self.store.get("key"), b"value")
        self.clock.advance(1)
        self.assertEqual(self.store.sweep(), 1)
        self.assertIsNone(self.store.get("key"))

    def test_clearing_ttls_removes_timers_immediately(self) -> None:
        for i in range(2_000):
            self.store.set(str(i), b"value")
            self.store.expire(str(i), 100_000)
        for i in range(2_000):
            self.store.set(str(i), b"replacement")
        self.assertEqual(len(self.store._expiry), 0)
        self.clock.advance(100_000)
        self.assertEqual(self.store.sweep(), 0)
        self.assertEqual(self.store.get("0"), b"replacement")


if __name__ == "__main__":
    unittest.main()
