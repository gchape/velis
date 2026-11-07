import unittest

from tests.helpers import ManualClock
from velis.core.store import MAX_TTL_MS, Store


class StoreTests(unittest.TestCase):
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

    def test_sweep_ignores_stale_heap_entries(self) -> None:
        """Overwriting a value clears its TTL and invalidates the old heap entry."""
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
                with self.assertRaisesRegex(ValueError, "ttl out of range"):
                    self.store.expire("key", ms)
                self.assertEqual(self.store.get("key"), b"value")
        self.clock.advance(100)
        self.assertEqual(self.store.sweep(), 1)
        self.assertIsNone(self.store.get("key"))

    def test_invalid_ttl_is_rejected_for_missing_keys(self) -> None:
        for ms in (-1, MAX_TTL_MS + 1):
            with self.subTest(ms=ms), self.assertRaises(ValueError):
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

    def test_repeated_ttl_updates_bound_heap_and_preserve_latest_expiry(self) -> None:
        self.store.set("key", b"value")
        for ms in range(10_000, 20_000):
            self.store.expire("key", ms)
        self.assertLessEqual(len(self.store._expiry_heap), 1024)
        self.clock.advance(19_998)
        self.assertEqual(self.store.get("key"), b"value")
        self.clock.advance(1)
        self.assertEqual(self.store.sweep(), 1)
        self.assertIsNone(self.store.get("key"))

    def test_clearing_ttls_reclaims_stale_heap_entries(self) -> None:
        for i in range(2_000):
            self.store.set(str(i), b"value")
            self.store.expire(str(i), 100_000)
        for i in range(2_000):
            self.store.set(str(i), b"replacement")
        self.assertLessEqual(len(self.store._expiry_heap), 1024)
        self.clock.advance(100_000)
        self.assertEqual(self.store.sweep(), 0)
        self.assertEqual(self.store.get("0"), b"replacement")


if __name__ == "__main__":
    unittest.main()
