import unittest
from unittest.mock import patch

from velis.core.store import Store


class SystemClockTests(unittest.TestCase):
    def test_wall_clock_changes_do_not_change_ttl(self) -> None:
        with (
            patch("time.monotonic_ns", return_value=1_000_000_000) as monotonic,
            patch("time.time_ns", return_value=1_000_000_000) as wall,
        ):
            store = Store()
            store.set("key", b"value")
            store.expire("key", 500)
            wall.return_value = 1_000_000_000_000
            monotonic.return_value = 1_499_000_000
            self.assertEqual(store.get("key"), b"value")
            wall.return_value = 0
            monotonic.return_value = 1_500_000_000
            self.assertIsNone(store.get("key"))
