import unittest
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

from velis.core.clock import MonotonicClock
from velis.core.expiry_index import ExpiryIndex
from velis.core.membership import Memberships


class ExpiryIndexTests(unittest.TestCase):
    def test_unchanged_deadline_does_not_accumulate_heap_entries(self) -> None:
        index = ExpiryIndex()
        for _ in range(1000):
            index.set("key", 10)
        self.assertEqual(len(index._heap), 1)
        self.assertEqual(index.pop_due(10, 10), ["key"])
        index.set("key", 10)
        self.assertEqual(index.pop_due(10, 10), ["key"])

    def test_future_stale_timers_are_not_scanned_until_due(self) -> None:
        index = ExpiryIndex()
        for i in range(100):
            index.set(str(i), 100 + i)
        for i in range(100):
            index.set(str(i), 1000 + i)
        with patch("velis.core.expiry_index.heappop", side_effect=AssertionError):
            self.assertFalse(index.has_due(99))
            self.assertEqual(index.pop_due(99, 10), [])
        self.assertFalse(index.has_due(999))
        self.assertEqual(index.pop_due(1000, 10), ["0"])

    def test_deadline_order_and_bounded_pop(self) -> None:
        index = ExpiryIndex()
        for key, deadline in (("later", 20), ("early", 5), ("middle", 10)):
            index.set(key, deadline)
        self.assertFalse(index.has_due(4))
        self.assertEqual(index.pop_due(10, 0), [])
        self.assertEqual(index.pop_due(10, 1), ["early"])
        self.assertIsNone(index.deadline("early"))
        self.assertTrue(index.has_due(10))
        self.assertEqual(index.pop_due(10, 10), ["middle"])
        self.assertFalse(index.has_due(10))
        self.assertEqual(index.pop_due(20, 10), ["later"])
        self.assertFalse(index.has_due(100))

    def test_cleared_and_rescheduled_heap_entries_never_fire(self) -> None:
        index = ExpiryIndex()
        index.set("cleared", 0)
        self.assertTrue(index.clear("cleared"))
        self.assertFalse(index.clear("cleared"))
        index.set("moved", 1)
        index.set("moved", 10)
        index.set("moved", 10)
        self.assertFalse(index.has_due(9))
        self.assertEqual(index.pop_due(10, 10), ["moved"])
        self.assertFalse(index.has_due(100))

    def test_heavy_rescheduling_and_compaction_preserve_live_deadlines(self) -> None:
        index = ExpiryIndex()
        index.set("other", 2000)
        for deadline in range(1000):
            index.set("moving", deadline)
        self.assertEqual(index.deadline("moving"), 999)
        self.assertEqual(index.pop_due(998, 10), [])
        self.assertEqual(index.pop_due(999, 10), ["moving"])
        self.assertEqual(index.pop_due(2000, 10), ["other"])

    def test_cancelling_timers_releases_stale_metadata_behind_a_live_head(self) -> None:
        index = ExpiryIndex()
        index.set("keep", 1)
        for i in range(1000):
            index.set(str(i), 1000 + i)
        for i in range(1000):
            self.assertTrue(index.clear(str(i)))
        # A live earlier timer prevents head trimming from reclaiming this garbage.
        self.assertLess(len(index._heap), 100)
        self.assertEqual(index.pop_due(1, 1), ["keep"])
        self.assertFalse(index.has_due(10_000))

    @given(
        st.lists(st.tuples(st.sampled_from("abc"), st.integers(0, 100)), max_size=100)
    )
    def test_only_latest_deadline_per_key_is_returned(
        self, updates: list[tuple[str, int]]
    ) -> None:
        index = ExpiryIndex()
        expected = dict(updates)
        for key, deadline in updates:
            index.set(key, deadline)
        self.assertEqual(
            index.pop_due(100, 100), sorted(expected, key=lambda k: (expected[k], k))
        )
        self.assertFalse(index.has_due(100))


class MembershipTests(unittest.TestCase):
    def test_join_leave_and_leave_all_keep_both_indexes_consistent(self) -> None:
        memberships = Memberships[str, str]()
        memberships.join("a", "one")
        memberships.join("a", "one")
        memberships.join("b", "one")
        memberships.join("a", "two")
        self.assertEqual(memberships.group_count("one"), 2)
        self.assertEqual(memberships.member_count("a"), 2)
        memberships.leave("a", "one")
        memberships.leave("missing", "one")
        self.assertFalse(memberships.is_member("a", "one"))
        self.assertEqual(memberships.group_count("one"), 1)
        memberships.leave_all("one")
        memberships.leave_all("one")
        self.assertEqual(memberships.members("b"), ())
        self.assertEqual(memberships.group_count("one"), 0)
        self.assertEqual(memberships.members("a"), ("two",))

    def test_members_are_a_snapshot_safe_for_mutation_during_delivery(self) -> None:
        memberships = Memberships[str, int]()
        for member in range(3):
            memberships.join("a", member)
        snapshot = memberships.members("a")
        for member in snapshot:
            memberships.leave_all(member)
        self.assertEqual(set(snapshot), {0, 1, 2})
        self.assertEqual(memberships.member_count("a"), 0)


class ClockTests(unittest.TestCase):
    def test_monotonic_nanoseconds_are_floored_to_milliseconds(self) -> None:
        with patch("velis.core.clock.time.monotonic_ns", return_value=1_234_999_999):
            self.assertEqual(MonotonicClock.now_ms(), 1234)
