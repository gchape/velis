import random
import unittest

from velis.core.expiry_index import ExpiryIndex


class ExpiryIndexTests(unittest.TestCase):
    def test_replaced_and_cleared_deadlines_leave_one_live_timer(self) -> None:
        index = ExpiryIndex()
        index.set("updated", 10)
        index.set("updated", 30)
        index.set("cleared", 10)
        self.assertTrue(index.clear("cleared"))
        self.assertFalse(index.clear("cleared"))
        self.assertEqual(index.pop_due(10, 100), [])
        self.assertEqual(index.deadline("updated"), 30)
        self.assertEqual(index.pop_due(30, 100), ["updated"])
        self.assertIsNone(index.deadline("updated"))
        self.assertFalse(index.has_due(30))

    def test_cancelled_keys_with_equal_deadlines_do_not_consume_budget(self) -> None:
        index = ExpiryIndex()
        index.set("a", 0)
        index.set("b", 0)
        index.clear("a")
        self.assertEqual(index.pop_due(0, 1), ["b"])
        self.assertFalse(index.has_due(0))

    def test_stale_entries_do_not_consume_the_expiry_budget(self) -> None:
        index = ExpiryIndex()
        index.set("a", 0)
        index.set("b", 1)
        index.clear("a")
        self.assertEqual(index.pop_due(1, 1), ["b"])
        self.assertFalse(index.has_due(1))

    def test_rescheduling_and_cancellation_keep_stale_storage_bounded(self) -> None:
        index = ExpiryIndex()
        for deadline in range(10_000):
            index.set("changing", deadline)
            index.set("deleted", deadline)
            index.clear("deleted")
        self.assertEqual(len(index._deadlines), 1)
        self.assertLessEqual(len(index._heap), 2 * (len(index._deadlines) + 1) + 64)
        self.assertEqual(index.pop_due(9_998, 1), [])
        self.assertEqual(index.pop_due(9_999, 100), ["changing"])

    def test_readiness_skips_mass_cancellation_before_a_future_timer(self) -> None:
        index = ExpiryIndex()
        for number in range(5000):
            index.set(str(number), number)
        for number in range(4999):
            self.assertTrue(index.clear(str(number)))
        self.assertEqual(len(index._deadlines), 1)
        self.assertFalse(index.has_due(4998))
        self.assertEqual(len(index._heap), 1)
        self.assertEqual(index.pop_due(4999, 1), ["4999"])

    def test_exact_deadlines_at_negative_and_large_clock_origins(self) -> None:
        for origin in (-5000, 0, 1003, 1 << 80):
            for delay in (0, 1, 63, 64, 65, 4095, 4096, 1 << 62):
                with self.subTest(origin=origin, delay=delay):
                    index = ExpiryIndex()
                    deadline = origin + delay
                    index.set("key", deadline)
                    self.assertFalse(index.has_due(deadline - 1))
                    self.assertEqual(index.pop_due(deadline - 1, 100), [])
                    self.assertEqual(index.pop_due(deadline, 100), ["key"])

    def test_peeking_future_timers_allows_earlier_and_backdated_insertions(
        self,
    ) -> None:
        index = ExpiryIndex()
        index.set("future", 1 << 62)
        self.assertFalse(index.has_due(0))
        self.assertEqual(index.pop_due(0, 1), [])
        index.set("soon", 1)
        self.assertEqual(index.pop_due(1, 1), ["soon"])
        index.set("backdated", -5)
        self.assertEqual(index.pop_due(1, 1), ["backdated"])
        self.assertEqual(index.deadline("future"), 1 << 62)

    def test_duplicate_deadlines_are_returned_once_with_bounded_batches(self) -> None:
        index = ExpiryIndex()
        for number in range(100):
            index.set(str(number), 1 << 62)
            index.set(str(number), 1 << 62)
        for budget in (0, -1):
            self.assertEqual(index.pop_due(1 << 63, budget), [])
            self.assertEqual(len(index._deadlines), 100)
        expired: list[str] = []
        while index.has_due(1 << 63):
            batch = index.pop_due(1 << 63, 7)
            self.assertLessEqual(len(batch), 7)
            expired.extend(batch)
        self.assertEqual(len(expired), 100)
        self.assertEqual(set(expired), {str(number) for number in range(100)})
        index.set("reused", -1)
        self.assertEqual(index.pop_due(0, 1), ["reused"])

    def test_rescheduling_after_partial_drain_removes_the_old_deadline(self) -> None:
        index = ExpiryIndex()
        for key in ("a", "b", "c"):
            index.set(key, 65)
        expired = index.pop_due(65, 1)
        rescheduled, cancelled = sorted({"a", "b", "c"} - set(expired))
        index.set(rescheduled, 1000)
        index.clear(cancelled)
        self.assertFalse(index.has_due(65))
        self.assertEqual(index.pop_due(65, 2), [])
        self.assertFalse(index.has_due(65))
        self.assertEqual(index.pop_due(1000, 1), [rescheduled])

    def test_updates_and_partial_drains_match_a_scanning_deadline_model(self) -> None:
        for origin in (-5000, 0, 1003, 1 << 80):
            for seed in range(4):
                rng = random.Random(seed)
                index = ExpiryIndex()
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
                                index.set(key, deadline)
                                model[key] = deadline
                            case 2:
                                self.assertEqual(
                                    index.clear(key), model.pop(key, None) is not None
                                )
                            case 3:
                                now += rng.choice((0, 1, 64, 4096, 1 << 62))
                            case 4:
                                budget = rng.randrange(5)
                                expected = sorted(d for d in model.values() if d <= now)
                                due = index.pop_due(now, budget)
                                actual = [model.pop(expired) for expired in due]
                                self.assertEqual(actual, expected[:budget])
                        self.assertEqual(index._deadlines, model)
                        self.assertEqual(
                            index.has_due(now), any(d <= now for d in model.values())
                        )
                        for live_key, deadline in model.items():
                            self.assertEqual(index.deadline(live_key), deadline)
                while index.has_due(now):
                    for expired in index.pop_due(now, 3):
                        self.assertLessEqual(model.pop(expired), now)
                self.assertTrue(all(deadline > now for deadline in model.values()))

    def test_readiness_ignores_rescheduled_deadlines(
        self,
    ) -> None:
        index = ExpiryIndex()
        index.set("key", 1)
        index.set("key", 100)
        self.assertFalse(index.has_due(1))
        self.assertEqual(index.pop_due(1, 1), [])
        self.assertFalse(index.has_due(1))
        self.assertEqual(index.deadline("key"), 100)

    def test_reinsertion_at_the_same_deadline_expires_the_key_once(self) -> None:
        index = ExpiryIndex()
        index.set("other", 100)
        index.set("key", 1)
        index.clear("key")
        index.set("key", 1)
        self.assertEqual(index.pop_due(1, 1), ["key"])
        self.assertIsNone(index.deadline("key"))
        self.assertEqual(index.pop_due(1, 1), [])
        self.assertEqual(index.deadline("other"), 100)

    def test_unchanged_deadlines_remain_bounded_and_expire_the_key_once(self) -> None:
        index = ExpiryIndex()
        for _ in range(10000):
            index.set("key", 1)
        self.assertLessEqual(len(index._heap), 66)
        self.assertEqual(index.pop_due(1, 1), ["key"])
        self.assertEqual(index.pop_due(1, 100), [])
        self.assertFalse(index.has_due(1))

    def test_readiness_cleans_stale_entries_after_last_cancellation(self) -> None:
        index = ExpiryIndex()
        for deadline in range(50):
            index.set("key", deadline)
        index.clear("key")
        self.assertFalse(index._deadlines)
        self.assertLessEqual(len(index._heap), 64)
        self.assertFalse(index.has_due(100))
        self.assertEqual(index.pop_due(100, 7), [])
        self.assertFalse(index._heap)
        index.set("key", -1)
        self.assertEqual(index.pop_due(0, 1), ["key"])

    def test_due_keys_are_returned_in_deadline_order_with_bounded_batches(self) -> None:
        index = ExpiryIndex()
        for key, deadline in (("c", 19), ("a", 10), ("b", 15)):
            index.set(key, deadline)
        self.assertEqual(index.pop_due(9, 100), [])
        self.assertEqual(index.pop_due(18, 1), ["a"])
        self.assertTrue(index.has_due(18))
        self.assertEqual(index.pop_due(18, 2), ["b"])
        self.assertFalse(index.has_due(18))
        self.assertEqual(index.pop_due(19, 2), ["c"])
        self.assertFalse(index.has_due(19))
