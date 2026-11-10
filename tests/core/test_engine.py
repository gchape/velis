import unittest

from tests.helpers import ManualClock, RecordingSubscriber
from velis.config import StoreLimits
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.messages import (
    NIL,
    OK,
    UNLIMITED,
    Keep,
    Lifetime,
    Message,
    Read,
    Remaining,
    RemainingValue,
    Remove,
    Send,
    Sent,
    Unwatch,
    Unwatched,
    Value,
    Watch,
    Watching,
    Write,
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
            (Read("k"), NIL),
            (Remaining("k"), NIL),
            (Remove("k"), NIL),
            (Lifetime("k", 5), NIL),
            (Keep("k"), NIL),
            (Write("k", b""), OK),
            (Read("k"), Value(b"")),
            (Remaining("k"), UNLIMITED),
            (Keep("k"), NIL),
            (Lifetime("k", 5), OK),
            (Remaining("k"), RemainingValue(5)),
            (Keep("k"), OK),
            (Remaining("k"), UNLIMITED),
            (Remove("k"), OK),
            (Remove("k"), NIL),
        ):
            with self.subTest(command=command):
                self.assertEqual(self.engine.execute(command, self.subscriber), reply)

    def test_pubsub_and_disconnect(self) -> None:
        self.assertEqual(
            self.engine.execute(Watch("a"), self.subscriber), Watching("a")
        )
        self.assertEqual(self.engine.execute(Send("a", b"v"), self.subscriber), Sent(1))
        self.assertEqual(self.subscriber.messages, [Message("a", b"v")])
        self.assertEqual(
            self.engine.execute(Unwatch("a"), self.subscriber), Unwatched("a")
        )
        self.engine.execute(Watch("a"), self.subscriber)
        self.engine.execute(Watch("b"), self.subscriber)
        self.engine.disconnect(self.subscriber)
        self.engine.disconnect(self.subscriber)
        for channel in "ab":
            self.assertEqual(
                self.engine.execute(Send(channel, b"v"), self.subscriber),
                Sent(0),
            )

    def test_command_errors_propagate_to_the_transport(self) -> None:
        engine = Engine(
            Store(limits=StoreLimits(max_bytes=1)), Broker(max_subscriptions=1)
        )
        with self.assertRaises(CapacityError):
            engine.execute(Write("k", b"v"), self.subscriber)
        with self.assertRaises(CommandError):
            engine.execute(Lifetime("missing", MAX_TTL_MS + 1), self.subscriber)
        engine.execute(Watch("a"), self.subscriber)
        with self.assertRaises(CapacityError):
            engine.execute(Watch("b"), self.subscriber)

    def test_maintenance_delegates_bounded_sweeps(self) -> None:
        for key in "ab":
            self.engine.execute(Write(key, b"v"), self.subscriber)
            self.engine.execute(Lifetime(key, 0), self.subscriber)
        self.assertTrue(self.engine.has_due_expirations)
        self.assertEqual(self.engine.sweep(1), 1)
        self.assertTrue(self.engine.has_due_expirations)
        self.assertEqual(self.engine.sweep(), 1)
        self.assertFalse(self.engine.has_due_expirations)
