import unittest

from velis.core.expiry import ExpiryIndex


class ExpiryIndexTests(unittest.TestCase):
    def test_replaced_and_cleared_deadlines_ignore_stale_entries(self) -> None:
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

    def test_stale_entries_consume_the_bounded_work_budget(self) -> None:
        index = ExpiryIndex()
        index.set("a", 0)
        index.clear("a")
        index.set("b", 0)
        self.assertEqual(index.pop_due(0, 1), [])
        self.assertTrue(index.has_due(0))
        self.assertEqual(index.pop_due(0, 1), ["b"])
        self.assertFalse(index.has_due(0))

    def test_duplicate_deadlines_yield_a_key_only_once(self) -> None:
        index = ExpiryIndex()
        index.set("key", 5)
        index.set("key", 5)
        self.assertEqual(index.pop_due(4, 10), [])
        self.assertEqual(index.pop_due(5, 10), ["key"])

    def test_compaction_preserves_current_deadlines(self) -> None:
        index = ExpiryIndex()
        for deadline in range(10_000):
            index.set("key", deadline)
        self.assertLessEqual(len(index._heap), 1024)
        self.assertEqual(index.pop_due(9_998, 10_000), [])
        self.assertEqual(index.pop_due(9_999, 10_000), ["key"])
