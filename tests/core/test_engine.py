import unittest

from tests.helpers import ManualClock, RecordingSubscriber
from velis.config import StoreLimits
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.messages import (
    NIL,
    OK,
    PERSISTENT,
    TTL,
    Delete,
    Delivered,
    Expire,
    Get,
    Message,
    Persist,
    Publish,
    Set,
    Subscribe,
    Subscribed,
    TTLValue,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.core.store import MAX_TTL_MS, Store
from velis.errors import CapacityError, CommandError


class EngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.engine = Engine(Store(self.clock), Broker())
        self.subscriber = RecordingSubscriber()

    def test_cache_commands_translate_store_results(self) -> None:
        for command, reply in (
            (Get("k"), NIL),
            (TTL("k"), NIL),
            (Delete("k"), NIL),
            (Expire("k", 5), NIL),
            (Persist("k"), NIL),
            (Set("k", b""), OK),
            (Get("k"), Value(b"")),
            (TTL("k"), PERSISTENT),
            (Persist("k"), NIL),
            (Expire("k", 5), OK),
            (TTL("k"), TTLValue(5)),
            (Persist("k"), OK),
            (TTL("k"), PERSISTENT),
            (Delete("k"), OK),
            (Delete("k"), NIL),
        ):
            with self.subTest(command=command):
                self.assertEqual(self.engine.execute(command, self.subscriber), reply)

    def test_pubsub_and_disconnect(self) -> None:
        self.assertEqual(
            self.engine.execute(Subscribe("a"), self.subscriber), Subscribed("a")
        )
        self.assertEqual(
            self.engine.execute(Publish("a", b"v"), self.subscriber), Delivered(1)
        )
        self.assertEqual(self.subscriber.messages, [Message("a", b"v")])
        self.assertEqual(
            self.engine.execute(Unsubscribe("a"), self.subscriber), Unsubscribed("a")
        )
        self.engine.execute(Subscribe("a"), self.subscriber)
        self.engine.execute(Subscribe("b"), self.subscriber)
        self.engine.disconnect(self.subscriber)
        self.engine.disconnect(self.subscriber)
        for channel in "ab":
            self.assertEqual(
                self.engine.execute(Publish(channel, b"v"), self.subscriber),
                Delivered(0),
            )

    def test_command_errors_propagate_to_the_transport(self) -> None:
        engine = Engine(
            Store(limits=StoreLimits(max_bytes=1)), Broker(max_subscriptions=1)
        )
        with self.assertRaises(CapacityError):
            engine.execute(Set("k", b"v"), self.subscriber)
        with self.assertRaises(CommandError):
            engine.execute(Expire("missing", MAX_TTL_MS + 1), self.subscriber)
        engine.execute(Subscribe("a"), self.subscriber)
        with self.assertRaises(CapacityError):
            engine.execute(Subscribe("b"), self.subscriber)

    def test_maintenance_delegates_bounded_sweeps(self) -> None:
        for key in "ab":
            self.engine.execute(Set(key, b"v"), self.subscriber)
            self.engine.execute(Expire(key, 0), self.subscriber)
        self.assertTrue(self.engine.has_due_expirations)
        self.assertEqual(self.engine.sweep(1), 1)
        self.assertTrue(self.engine.has_due_expirations)
        self.assertEqual(self.engine.sweep(), 1)
        self.assertFalse(self.engine.has_due_expirations)
