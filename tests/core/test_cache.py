import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.helpers import ManualClock, RecordingSubscriber
from velis.app import build_engine
from velis.config import Config
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
    Unsubscribe,
    Value,
)
from velis.core.engine import Engine
from velis.core.store import Store


class CacheTests(unittest.TestCase):
    def test_restart_ignores_old_wal_and_starts_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            wal = Path(directory) / "cache.wal"
            original = b"existing cache WAL must be left untouched"
            wal.write_bytes(original)
            with patch.dict(
                os.environ,
                {
                    "VELIS_WAL_PATH": str(wal),
                    "VELIS_FSYNC": "obsolete",
                    "VELIS_COMPACT_BYTES": "obsolete",
                },
            ):
                Config.from_env()
                sub = RecordingSubscriber()
                engine = build_engine()
                self.assertEqual(engine.execute(Set("k", b"v"), sub), Ok())
                self.assertEqual(engine.execute(Expire("k", 5000), sub), Ok())
                self.assertEqual(engine.execute(Get("k"), sub), Value(b"v"))
                self.assertEqual(build_engine().execute(Get("k"), sub), Nil())
            self.assertEqual(wal.read_bytes(), original)
            self.assertEqual(list(Path(directory).iterdir()), [wal])

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
