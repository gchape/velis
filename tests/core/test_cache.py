import unittest

from tests.helpers import ManualClock, RecordingSubscriber
from velis.app import build_engine
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.messages import (
    Delivered,
    Expire,
    Get,
    Nil,
    Ok,
    Publish,
    Set,
    Subscribe,
    Unsubscribe,
    Value,
)
from velis.core.store import Store


class CacheTests(unittest.TestCase):
    def test_new_engine_starts_with_an_empty_store(self) -> None:
        sub = RecordingSubscriber()
        engine = build_engine()
        self.assertEqual(engine.execute(Set("k", b"v"), sub), Ok())
        self.assertEqual(engine.execute(Expire("k", 5000), sub), Ok())
        self.assertEqual(engine.execute(Get("k"), sub), Value(b"v"))
        self.assertEqual(build_engine().execute(Get("k"), sub), Nil())

    def test_ttl_reset_lazy_expiry_and_sweep(self) -> None:
        clock = ManualClock()
        engine = Engine(Store(clock), Broker())
        sub = RecordingSubscriber()
        engine.execute(Set("reset", b"old"), sub)
        engine.execute(Expire("reset", 10), sub)
        engine.execute(Set("reset", b"new"), sub)
        for key in ("lazy", "swept"):
            engine.execute(Set(key, b"v"), sub)
            engine.execute(Expire(key, 10), sub)
        clock.advance(10)
        self.assertEqual(engine.execute(Get("lazy"), sub), Nil())
        self.assertEqual(engine.sweep(), 1)
        self.assertEqual(engine.execute(Get("swept"), sub), Nil())
        self.assertEqual(engine.execute(Get("reset"), sub), Value(b"new"))

    def test_pubsub_delivery_and_disconnect(self) -> None:
        engine = build_engine()
        first, second = RecordingSubscriber(), RecordingSubscriber()
        for sub in (first, second):
            engine.execute(Subscribe("news"), sub)
        self.assertEqual(engine.execute(Publish("news", b"hi"), first), Delivered(2))
        self.assertEqual(first.messages, [("news", b"hi")])
        self.assertEqual(second.messages, first.messages)
        engine.execute(Unsubscribe("news"), first)
        self.assertEqual(engine.execute(Publish("news", b"next"), first), Delivered(1))
        engine.disconnect(second)
        self.assertEqual(engine.execute(Publish("news", b"gone"), first), Delivered(0))


if __name__ == "__main__":
    unittest.main()
