import unittest
from unittest.mock import Mock

from tests.helpers import ManualClock
from velis.config import StoreLimits
from velis.core.evicting import Evicting
from velis.core.eviction import EvictionPolicy
from velis.core.expiring import Expiring, TtlStatus
from velis.core.kv import Memory
from velis.errors import CapacityError


class StoreLayerTests(unittest.TestCase):
    def test_memory_accounts_for_unicode_keys_empty_values_and_replacements(
        self,
    ) -> None:
        memory = Memory()
        memory.set("é", b"123")
        memory.set("x", b"")
        self.assertEqual(memory.stored_bytes, 6)
        memory.set("é", b"")
        self.assertEqual(memory.stored_bytes, 3)
        self.assertEqual(memory.get("é"), b"")
        self.assertIn("é", memory)
        self.assertEqual(len(memory), 2)
        self.assertTrue(memory.delete("é"))
        self.assertFalse(memory.delete("é"))
        self.assertNotIn("é", memory)
        self.assertIsNone(memory.get("é"))
        self.assertEqual(memory.stored_bytes, 1)

    def test_capacity_reclaims_once_and_reports_each_victim_after_removal(self) -> None:
        memory = Memory()
        evicting = Evicting(
            memory, StoreLimits(max_bytes=8, eviction_policy=EvictionPolicy.LRU)
        )
        for key in ("a", "b", "c"):
            evicting.set(key, b"1")
        events: list[str] = []

        def reclaim() -> None:
            events.append("reclaim")
            self.assertTrue(evicting.delete("a"))

        def on_evict(key: str) -> None:
            self.assertNotIn(key, memory)
            events.append(key)

        evicting.reclaim = reclaim
        evicting.on_evict = on_evict
        evicting.set("d", b"1234567")
        self.assertEqual(events, ["reclaim", "b", "c"])
        self.assertEqual(evicting.evictions, 2)
        self.assertEqual(evicting.stored_bytes, 8)
        self.assertEqual(len(evicting), 1)
        self.assertEqual(evicting.get("d"), b"1234567")

    def test_capacity_rejects_oversized_values_before_deletion_or_hooks(self) -> None:
        evicting = Evicting(Memory(), StoreLimits(max_bytes=2))
        evicting.set("a", b"1")
        evicting.reclaim = Mock()
        evicting.on_evict = Mock()
        with self.assertRaises(CapacityError):
            evicting.set("a", b"12")
        self.assertEqual(evicting.get("a"), b"1")
        self.assertEqual(evicting.evictions, 0)
        evicting.reclaim.assert_not_called()
        evicting.on_evict.assert_not_called()

    def test_expiry_reclaims_capacity_and_cancels_capacity_victim_timers(self) -> None:
        clock = ManualClock()
        evicting = Evicting(Memory(), StoreLimits(max_keys=1))
        expiring = Expiring(evicting, clock)
        expiring.set("a", b"old")
        expiring.expire("a", 10)
        expiring.set("b", b"live")
        self.assertEqual(evicting.evictions, 1)
        self.assertFalse(expiring.has_due_expirations)
        expiring.set("a", b"fresh")
        clock.advance(10)
        self.assertEqual(expiring.sweep(), 0)
        self.assertEqual(expiring.get("a"), b"fresh")
        self.assertIs(expiring.ttl("a"), TtlStatus.PERSISTENT)
        expiring.expire("a", 0)
        expiring.set("c", b"new")
        self.assertEqual(expiring.expirations, 1)
        self.assertEqual(evicting.evictions, 2)
        self.assertEqual(expiring.get("c"), b"new")
