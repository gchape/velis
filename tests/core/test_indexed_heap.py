import random
import unittest

from velis.core.indexed_heap import IndexedBinaryHeap


class IndexedBinaryHeapTests(unittest.TestCase):
    def test_replaced_and_cleared_deadlines_leave_one_live_timer(self) -> None:
        heap = IndexedBinaryHeap()
        heap.set("updated", 10)
        heap.set("updated", 30)
        heap.set("cleared", 10)
        self.assertTrue(heap.clear("cleared"))
        self.assertFalse(heap.clear("cleared"))
        self.assertEqual(heap.pop_due(10, 100), [])
        self.assertEqual(heap.deadline("updated"), 30)
        self.assertEqual(heap.pop_due(30, 100), ["updated"])
        self.assertIsNone(heap.deadline("updated"))
        self.assertFalse(heap.has_due(30))

    def test_cancellation_does_not_consume_the_expiry_budget(self) -> None:
        heap = IndexedBinaryHeap()
        heap.set("a", 0)
        heap.clear("a")
        heap.set("b", 0)
        self.assertEqual(heap.pop_due(0, 1), ["b"])

    def test_rescheduling_and_cancellation_leave_no_stale_entries(self) -> None:
        heap = IndexedBinaryHeap()
        for deadline in range(10_000):
            heap.set("changing", deadline)
            heap.set("deleted", deadline)
            heap.clear("deleted")
        self.assertEqual(len(heap), 1)
        self.assertEqual(len(heap._heap), 1)
        self.assertEqual(heap.pop_due(9_998, 1), [])
        self.assertEqual(heap.pop_due(9_999, 1), ["changing"])

    def test_clearing_deadlines_unlinks_entries_immediately(self) -> None:
        heap = IndexedBinaryHeap()
        for index in range(5000):
            heap.set(str(index), index)
        for index in range(4999):
            self.assertTrue(heap.clear(str(index)))
        self.assertEqual(len(heap), 1)
        self.assertEqual(len(heap._heap), 1)
        self.assertEqual(heap.pop_due(4999, 1), ["4999"])

    def test_exact_deadlines_at_negative_and_large_clock_origins(self) -> None:
        for origin in (-5000, 0, 1003, 1 << 80):
            for delay in (0, 1, 63, 64, 65, 4095, 4096, 1 << 62):
                with self.subTest(origin=origin, delay=delay):
                    heap = IndexedBinaryHeap()
                    deadline = origin + delay
                    heap.set("key", deadline)
                    self.assertFalse(heap.has_due(deadline - 1))
                    self.assertEqual(heap.pop_due(deadline - 1, 100), [])
                    self.assertEqual(heap.pop_due(deadline, 100), ["key"])

    def test_peeking_future_timers_allows_earlier_and_backdated_insertions(
        self,
    ) -> None:
        heap = IndexedBinaryHeap()
        heap.set("future", 1 << 62)
        self.assertFalse(heap.has_due(0))
        self.assertEqual(heap.pop_due(0, 1), [])
        heap.set("soon", 1)
        self.assertEqual(heap.pop_due(1, 1), ["soon"])
        heap.set("backdated", -5)
        self.assertEqual(heap.pop_due(1, 1), ["backdated"])
        self.assertEqual(heap.deadline("future"), 1 << 62)

    def test_duplicate_deadlines_are_returned_once_with_bounded_batches(self) -> None:
        heap = IndexedBinaryHeap()
        for index in range(100):
            heap.set(str(index), 1 << 62)
            heap.set(str(index), 1 << 62)
        for budget in (0, -1):
            self.assertEqual(heap.pop_due(1 << 63, budget), [])
            self.assertEqual(len(heap), 100)
        expired: list[str] = []
        while heap.has_due(1 << 63):
            batch = heap.pop_due(1 << 63, 7)
            self.assertLessEqual(len(batch), 7)
            expired.extend(batch)
        self.assertEqual(len(expired), 100)
        self.assertEqual(set(expired), {str(index) for index in range(100)})
        heap.set("reused", -1)
        self.assertEqual(heap.pop_due(0, 1), ["reused"])

    def test_rescheduling_after_partial_drain_removes_the_old_deadline(self) -> None:
        heap = IndexedBinaryHeap()
        for key in ("a", "b", "c"):
            heap.set(key, 65)
        expired = heap.pop_due(65, 1)
        rescheduled, cancelled = sorted({"a", "b", "c"} - set(expired))
        heap.set(rescheduled, 1000)
        heap.clear(cancelled)
        self.assertFalse(heap.has_due(65))
        self.assertEqual(heap.pop_due(1000, 1), [rescheduled])

    def test_updates_and_partial_drains_match_a_scanning_deadline_model(self) -> None:
        for origin in (-5000, 0, 1003, 1 << 80):
            for seed in range(4):
                rng = random.Random(seed)
                heap = IndexedBinaryHeap()
                now = origin
                model: dict[str, int] = {}
                for step in range(1500):
                    key = str(rng.randrange(64))
                    with self.subTest(origin=origin, seed=seed, step=step):
                        match rng.randrange(5):
                            case 0 | 1:
                                deadline = now + rng.choice(
                                    (-10, 0, 1, 63, 64, 65, 4096, 1 << 62)
                                )
                                heap.set(key, deadline)
                                model[key] = deadline
                            case 2:
                                self.assertEqual(
                                    heap.clear(key), model.pop(key, None) is not None
                                )
                            case 3:
                                now += rng.choice((0, 1, 64, 4096, 1 << 62))
                            case 4:
                                budget = rng.randrange(5)
                                expected = sorted(d for d in model.values() if d <= now)
                                due = heap.pop_due(now, budget)
                                actual = [model.pop(expired) for expired in due]
                                self.assertEqual(actual, expected[:budget])
                        self.assertEqual(len(heap), len(model))
                        self.assertEqual(len(heap._heap), len(model))
                        self.assertEqual(
                            heap.has_due(now), any(d <= now for d in model.values())
                        )
                        for index, entry in enumerate(heap._heap):
                            self.assertEqual(entry.index, index)
                            self.assertIs(heap._entries[entry.key], entry)
                            self.assertEqual(heap.deadline(entry.key), model[entry.key])
                            if index:
                                self.assertLessEqual(
                                    heap._heap[(index - 1) // 2].deadline,
                                    entry.deadline,
                                )
