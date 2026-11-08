import unittest
from unittest.mock import Mock

from tests.helpers import ManualClock, RecordingSubscriber
from velis.core.broker import Broker
from velis.core.commands import (
    Delivered,
    Expire,
    Get,
    Nil,
    Ok,
    Publish,
    Set,
    Subscribe,
    Subscribed,
    Value,
)
from velis.core.engine import Engine
from velis.core.store import Store
from velis.errors import CommandError


class EngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock(10_000)
        self.sub = RecordingSubscriber()

    def engine(self) -> Engine:
        return Engine(Store(self.clock), Broker())

    def test_basic_commands(self) -> None:
        e = self.engine()
        self.assertEqual(e.execute(Get("k"), self.sub), Nil())
        self.assertEqual(e.execute(Set("k", b"v"), self.sub), Ok())
        self.assertEqual(e.execute(Get("k"), self.sub), Value(b"v"))

    def test_expire_missing_key_is_nil(self) -> None:
        e = self.engine()
        self.assertEqual(e.execute(Expire("nope", 100), self.sub), Nil())

    def test_ttl_out_of_range(self) -> None:
        e = self.engine()
        e.execute(Set("k", b"v"), self.sub)
        for ms in (-1, 1 << 63):
            with self.subTest(ms=ms), self.assertRaises(CommandError):
                e.execute(Expire("k", ms), self.sub)

    def test_unexpected_store_value_error_is_not_a_command_error(self) -> None:
        store = Mock(spec=Store)
        store.expire.side_effect = ValueError("store bug")
        with self.assertRaisesRegex(ValueError, "store bug"):
            Engine(store, Broker()).execute(Expire("k", 1), self.sub)

    def test_pubsub(self) -> None:
        e = self.engine()
        self.assertEqual(e.execute(Subscribe("c"), self.sub), Subscribed("c"))
        self.assertEqual(e.execute(Publish("c", b"hi"), self.sub), Delivered(1))
        self.assertEqual(self.sub.messages, [("c", b"hi")])
        e.disconnect(self.sub)
        self.assertEqual(e.execute(Publish("c", b"hi"), self.sub), Delivered(0))

    def test_expire_uses_current_clock(self) -> None:
        e = self.engine()
        e.execute(Set("k", b"v"), self.sub)
        self.clock.advance(100)
        e.execute(Expire("k", 500), self.sub)
        self.clock.advance(499)
        self.assertEqual(e.execute(Get("k"), self.sub), Value(b"v"))
        self.clock.advance(1)
        self.assertEqual(e.execute(Get("k"), self.sub), Nil())

    def test_expire_replaces_previous_deadline(self) -> None:
        e = self.engine()
        e.execute(Set("k", b"v"), self.sub)
        e.execute(Expire("k", 100), self.sub)
        e.execute(Expire("k", 1_000), self.sub)
        self.clock.advance(500)
        e.sweep()
        self.assertEqual(e.execute(Get("k"), self.sub), Value(b"v"))
        self.clock.advance(500)
        self.assertEqual(e.execute(Get("k"), self.sub), Nil())


if __name__ == "__main__":
    unittest.main()
