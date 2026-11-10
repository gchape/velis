"""Exercise metadata reads and weakly consistent bounded key discovery."""

import unittest

from tests.helpers import ManualClock, RecordingSubscriber
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.eviction import EvictionPolicy
from velis.core.messages import (
    NIL,
    Exists,
    Found,
    KeyPage,
    ListKeys,
    Ping,
    Pong,
    Size,
    SizeValue,
)
from velis.core.store import LIST_SCAN_LIMIT, Store, StoreLimits, StoreStats
from velis.errors import CommandError


class InspectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.store = Store(self.clock)

    def test_existence_size_and_list_do_not_record_reads_or_change_eviction(
        self,
    ) -> None:
        store = Store(
            self.clock,
            limits=StoreLimits(max_keys=2, eviction_policy=EvictionPolicy.LRU),
        )
        store.set("a", b"value")
        store.set("b", b"")
        self.assertTrue(store.exists("a"))
        self.assertEqual(store.size("a"), 5)
        self.assertEqual(store.size("b"), 0)
        self.assertEqual(store.list_keys(), (0, ("a", "b")))
        self.assertEqual(store.stats(), StoreStats())
        store.set("c", b"")
        self.assertFalse(store.exists("a"))
        self.assertTrue(store.exists("b"))
        self.assertEqual(store.stats(), StoreStats(evictions=1))

    def test_inspection_lazily_purges_expired_values(self) -> None:
        for operation in ("exists", "size", "list_keys"):
            with self.subTest(operation=operation):
                store = Store(self.clock)
                store.set("expired", b"value")
                store.expire("expired", 0)
                result = getattr(store, operation)("expired")
                expected = {"exists": False, "size": None, "list_keys": (0, ())}[
                    operation
                ]
                self.assertEqual(result, expected)
                self.assertEqual(store.stats(), StoreStats(expirations=1))
                self.assertEqual(len(store), 0)

    def test_pages_preserve_insertion_order_and_prefix(self) -> None:
        for key in ("other", "a:second", "a:first", "last"):
            self.store.set(key, b"")
        cursor, keys = self.store.list_keys("a:", limit=1)
        self.assertNotEqual(cursor, 0)
        self.assertEqual(keys, ("a:second",))
        cursor, keys = self.store.list_keys("a:", cursor, 1)
        self.assertEqual(keys, ("a:first",))
        self.assertEqual(self.store.list_keys("a:", cursor, 1), (0, ()))
        self.assertEqual(self.store.list_keys(cursor=999), (0, ()))

    def test_scan_budget_can_return_empty_page_with_more_work(self) -> None:
        for index in range(LIST_SCAN_LIMIT):
            self.store.set(f"other:{index}", b"")
        self.store.set("match", b"")
        cursor, keys = self.store.list_keys("match")
        self.assertEqual(keys, ())
        self.assertNotEqual(cursor, 0)
        self.assertEqual(self.store.list_keys("match", cursor), (0, ("match",)))

    def test_expired_candidate_window_does_not_hide_the_next_live_key(self) -> None:
        for index in range(LIST_SCAN_LIMIT):
            key = f"expired:{index}"
            self.store.set(key, b"")
            self.store.expire(key, 0)
        self.store.set("live", b"")
        cursor, keys = self.store.list_keys()
        self.assertEqual(keys, ())
        self.assertNotEqual(cursor, 0)
        self.assertEqual(self.store.list_keys(cursor=cursor), (0, ("live",)))
        self.assertEqual(self.store.stats().expirations, LIST_SCAN_LIMIT)

    def test_invalid_page_bounds_leave_the_store_unchanged(self) -> None:
        self.store.set("key", b"value")
        for cursor, limit in ((-1, 1), (True, 1), (0, 0), (0, 101), (0, True)):
            with (
                self.subTest(cursor=cursor, limit=limit),
                self.assertRaises(CommandError),
            ):
                self.store.list_keys(cursor=cursor, limit=limit)
        self.assertEqual(self.store.stats(), StoreStats())
        self.assertEqual(len(self.store), 1)

    def test_engine_translates_inspection_results(self) -> None:
        self.store.set("key", b"value")
        engine = Engine(self.store, Broker())
        subscriber = RecordingSubscriber()
        for command, reply in (
            (Ping(), Pong()),
            (Exists("key"), Found()),
            (Exists("absent"), NIL),
            (Size("key"), SizeValue(5)),
            (Size("absent"), NIL),
            (ListKeys(), KeyPage(0, ("key",))),
        ):
            with self.subTest(command=command):
                self.assertEqual(engine.execute(command, subscriber), reply)
