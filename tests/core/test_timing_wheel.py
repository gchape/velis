import random
import unittest

from velis.core.timing_wheel import TimingWheel


class TimingWheelTests(unittest.TestCase):
    def test_replaced_and_cleared_deadlines_leave_one_live_timer(self) -> None:
        wheel = TimingWheel()
        wheel.set("updated", 10)
        wheel.set("updated", 30)
        wheel.set("cleared", 10)
        self.assertTrue(wheel.clear("cleared"))
        self.assertFalse(wheel.clear("cleared"))
        self.assertEqual(wheel.pop_due(10, 100), [])
        self.assertEqual(wheel.deadline("updated"), 30)
        self.assertEqual(wheel.pop_due(30, 100), ["updated"])
        self.assertIsNone(wheel.deadline("updated"))
        self.assertFalse(wheel.has_due(30))

    def test_cancellation_does_not_consume_the_expiry_budget(self) -> None:
        wheel = TimingWheel()
        wheel.set("a", 0)
        wheel.clear("a")
        wheel.set("b", 0)
        self.assertEqual(wheel.pop_due(0, 1), ["b"])
        self.assertFalse(wheel.has_due(0))

    def test_duplicate_deadlines_yield_a_key_only_once(self) -> None:
        wheel = TimingWheel()
        wheel.set("key", 5)
        wheel.set("key", 5)
        self.assertEqual(wheel.pop_due(4, 10), [])
        self.assertEqual(wheel.pop_due(5, 10), ["key"])

    def test_rescheduling_does_not_accumulate_old_timers_or_empty_buckets(self) -> None:
        wheel = TimingWheel()
        for deadline in range(10_000):
            wheel.set("key", deadline)
        self.assertEqual(len(wheel), 1)
        self.assertEqual(
            sum(
                len(bucket.keys) for slots in wheel._wheel for bucket in slots.values()
            ),
            1,
        )
        self.assertEqual(wheel.pop_due(9_998, 10_000), [])
        self.assertEqual(wheel.pop_due(9_999, 10_000), ["key"])

    def test_clearing_deadlines_unlinks_wheel_entries_immediately(self) -> None:
        wheel = TimingWheel()
        for i in range(5000):
            wheel.set(str(i), i)
        for i in range(4999):
            self.assertTrue(wheel.clear(str(i)))
        self.assertEqual(len(wheel), 1)
        self.assertEqual(wheel.deadline("4999"), 4999)
        self.assertEqual(wheel.pop_due(4999, 10_000), ["4999"])

    def test_exact_deadlines_across_levels_and_arbitrary_clock_origins(self) -> None:
        delays = {0, 1, 63, 64, 65, 127, 128, 4095, 4096, 4097, 1 << 62}
        delays.update(
            64**level + offset for level in range(1, 11) for offset in (-1, 0, 1)
        )
        for origin in (-5000, 0, 1003, 1 << 80):
            for delay in sorted(delays):
                with self.subTest(origin=origin, delay=delay):
                    wheel = TimingWheel(origin)
                    deadline = origin + delay
                    wheel.set("key", deadline)
                    if delay:
                        self.assertEqual(wheel.pop_due(deadline - 1, 100), [])
                        self.assertEqual(wheel.deadline("key"), deadline)
                    self.assertEqual(wheel.pop_due(deadline, 100), ["key"])
                    self.assertEqual(len(wheel), 0)
                    self.assertFalse(wheel.has_due(deadline))

    def test_a_coarse_bucket_never_expires_a_future_timer(self) -> None:
        wheel = TimingWheel()
        wheel.set("key", 65)
        self.assertTrue(wheel.has_due(64))
        self.assertEqual(wheel.pop_due(64, 1), [])
        self.assertFalse(wheel.has_due(64))
        self.assertEqual(wheel.pop_due(65, 1), ["key"])

    def test_cascading_counts_toward_the_work_budget(self) -> None:
        wheel = TimingWheel()
        for i in range(4):
            wheel.set(str(i), 65)
        wheel.set("due", 64)
        self.assertEqual(wheel.pop_due(64, 4), [])
        self.assertTrue(wheel.has_due(64))
        self.assertEqual(wheel.pop_due(64, 1), ["due"])
        self.assertFalse(wheel.has_due(64))
        self.assertEqual(wheel.pop_due(65, 2), ["0", "1"])
        self.assertTrue(wheel.has_due(65))
        self.assertEqual(wheel.pop_due(65, 2), ["2", "3"])

    def test_a_large_clock_jump_keeps_the_timer_budget(self) -> None:
        wheel = TimingWheel()
        for i in range(100):
            wheel.set(str(i), (1 << 62) + i)
        self.assertEqual(len(wheel.pop_due((1 << 63), 7)), 7)
        self.assertEqual(len(wheel), 93)
        self.assertTrue(wheel.has_due(1 << 63))
        self.assertEqual(len(wheel.pop_due((1 << 63), 100)), 93)
        self.assertFalse(wheel.has_due(1 << 63))

    def test_canceling_a_ready_bucket_preserves_a_reused_wheel_slot(self) -> None:
        wheel = TimingWheel()
        wheel.set("old", 65)
        self.assertTrue(wheel.has_due(64))
        self.assertTrue(wheel.has_due(4100))
        wheel.set("new", 4200)
        self.assertTrue(wheel.clear("old"))
        self.assertFalse(wheel.has_due(4100))
        self.assertEqual(wheel.pop_due(4200, 1), ["new"])

    def test_rescheduling_a_partly_drained_bucket_removes_the_old_deadline(
        self,
    ) -> None:
        wheel = TimingWheel()
        for key in ("a", "b", "c"):
            wheel.set(key, 65)
        self.assertEqual(wheel.pop_due(65, 1), ["a"])
        wheel.set("b", 1000)
        wheel.clear("c")
        self.assertFalse(wheel.has_due(65))
        self.assertEqual(wheel.pop_due(1000, 1), ["b"])

    def test_zero_budget_preserves_ready_work(self) -> None:
        wheel = TimingWheel()
        wheel.set("key", 1)
        self.assertEqual(wheel.pop_due(1, 0), [])
        self.assertTrue(wheel.has_due(1))
        self.assertEqual(wheel.pop_due(1, 1), ["key"])

    def test_wheel_matches_a_deadline_dictionary_under_random_operations(self) -> None:
        for seed in range(5):
            rng = random.Random(seed)
            now = 1003
            wheel = TimingWheel(now)
            model: dict[str, int] = {}
            for step in range(1000):
                key = str(rng.randrange(32))
                with self.subTest(seed=seed, step=step):
                    match rng.randrange(4):
                        case 0:
                            deadline = now + rng.choice(
                                (0, 1, 63, 64, 65, 4095, 4096, 1 << 62)
                            )
                            wheel.set(key, deadline)
                            model[key] = deadline
                        case 1:
                            self.assertEqual(
                                wheel.clear(key), model.pop(key, None) is not None
                            )
                        case 2:
                            now += rng.choice((0, 1, 63, 64, 4096, 1 << 62))
                        case 3:
                            budget = rng.randrange(1, 8)
                            due = wheel.pop_due(now, budget)
                            self.assertLessEqual(len(due), budget)
                            for expired in due:
                                self.assertLessEqual(model.pop(expired), now)
                    self.assertEqual(len(wheel), len(model))
                    for live_key, deadline in model.items():
                        self.assertEqual(wheel.deadline(live_key), deadline)
            while wheel.has_due(now):
                for expired in wheel.pop_due(now, 3):
                    self.assertLessEqual(model.pop(expired), now)
            self.assertTrue(all(deadline > now for deadline in model.values()))
            self.assertEqual(len(wheel), len(model))
