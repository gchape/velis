import os
import unittest
from unittest.mock import patch

from tests.helpers import RecordingSubscriber
from velis.app import build_engine
from velis.config import Cache, Config, Server
from velis.core.commands import Get, Nil, Set, Subscribe, Value
from velis.errors import CapacityError


class ConfigTests(unittest.TestCase):
    def test_default_environment_policy_is_lru(self) -> None:
        with patch.dict(
            os.environ,
            {"VELIS_MAX_KEYS": "1"},
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual(config.cache.eviction_policy, "lru")
        self.assertEqual(Cache().eviction_policy, "lru")
        engine = build_engine(config)
        subscriber = RecordingSubscriber()
        engine.execute(Set("a", b"first"), subscriber)
        engine.execute(Set("b", b"second"), subscriber)
        self.assertEqual(engine.execute(Get("a"), subscriber), Nil())
        self.assertEqual(engine.execute(Get("b"), subscriber), Value(b"second"))

    def test_invalid_eviction_policy_fails_at_startup(self) -> None:
        with (
            patch.dict(os.environ, {"VELIS_EVICTION_POLICY": "invalid"}, clear=True),
            self.assertRaisesRegex(ValueError, "eviction_policy"),
        ):
            Config.from_env()

    def test_environment_limits_reach_the_default_engine(self) -> None:
        with patch.dict(
            os.environ,
            {
                "VELIS_MAX_KEYS": "1",
                "VELIS_MAX_BYTES": "4",
                "VELIS_EVICTION_POLICY": "reject",
                "VELIS_MAX_CONNECTIONS": "2",
                "VELIS_MAX_SUBSCRIPTIONS": "1",
            },
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual(
            config.cache, Cache(max_keys=1, max_bytes=4, eviction_policy="reject")
        )
        self.assertEqual(config.server.max_connections, 2)
        engine = build_engine(config)
        subscriber = RecordingSubscriber()
        engine.execute(Set("a", b"123"), subscriber)
        with self.assertRaisesRegex(CapacityError, "store limit"):
            engine.execute(Set("b", b""), subscriber)
        with self.assertRaisesRegex(CapacityError, "byte limit"):
            engine.execute(Set("a", b"1234"), subscriber)
        engine.execute(Subscribe("a"), subscriber)
        with self.assertRaisesRegex(CapacityError, "subscription limit"):
            engine.execute(Subscribe("b"), subscriber)

    def test_environment_rejects_invalid_limits(self) -> None:
        for name in (
            "VELIS_MAX_KEYS",
            "VELIS_MAX_BYTES",
            "VELIS_MAX_CONNECTIONS",
            "VELIS_MAX_SUBSCRIPTIONS",
        ):
            for value in ("0", "-1", "invalid"):
                with (
                    self.subTest(name=name, value=value),
                    patch.dict(os.environ, {name: value}, clear=True),
                    self.assertRaises(ValueError),
                ):
                    Config.from_env()

    def test_server_rejects_nonpositive_connection_and_subscription_limits(
        self,
    ) -> None:
        for kwargs in ({"max_connections": 0}, {"max_subscriptions": -1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                Server("127.0.0.1", 0, **kwargs)
