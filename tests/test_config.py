import os
import unittest
from unittest.mock import patch

from tests.helpers import RecordingSubscriber
from velis.app import build_engine
from velis.config import Config, ServerConfig
from velis.core.messages import Get, Nil, Set, Subscribe, Value
from velis.core.store import EvictionPolicy, StoreLimits
from velis.errors import CapacityError


class ConfigTests(unittest.TestCase):
    def test_default_environment_policy_is_lru(self) -> None:
        with patch.dict(
            os.environ,
            {"VELIS_MAX_KEYS": "1"},
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual(config.store.eviction_policy, "lru")
        self.assertEqual(StoreLimits().eviction_policy, "lru")
        engine = build_engine(config)
        subscriber = RecordingSubscriber()
        engine.execute(Set("a", b"first"), subscriber)
        engine.execute(Set("b", b"second"), subscriber)
        self.assertEqual(engine.execute(Get("a"), subscriber), Nil())
        self.assertEqual(engine.execute(Get("b"), subscriber), Value(b"second"))

    def test_invalid_eviction_policy_fails_at_startup(self) -> None:
        with (
            patch.dict(os.environ, {"VELIS_EVICTION_POLICY": "invalid"}, clear=True),
            self.assertRaisesRegex(ValueError, "VELIS_EVICTION_POLICY"),
        ):
            Config.from_env()

    def test_sieve_environment_policy_reaches_the_engine(self) -> None:
        with patch.dict(
            os.environ,
            {"VELIS_EVICTION_POLICY": "sieve", "VELIS_MAX_KEYS": "2"},
            clear=True,
        ):
            config = Config.from_env()
        self.assertIs(config.store.eviction_policy, EvictionPolicy.SIEVE)
        engine = build_engine(config)
        subscriber = RecordingSubscriber()
        for key in ("a", "b"):
            engine.execute(Set(key, key.encode()), subscriber)
        self.assertEqual(engine.execute(Get("a"), subscriber), Value(b"a"))
        engine.execute(Set("c", b"c"), subscriber)
        self.assertEqual(engine.execute(Get("b"), subscriber), Nil())

    def test_removed_policies_fail_with_the_environment_variable_name(self) -> None:
        for policy in ("lfu", "fifo", "rr"):
            with (
                patch.dict(os.environ, {"VELIS_EVICTION_POLICY": policy}, clear=True),
                self.assertRaisesRegex(ValueError, "VELIS_EVICTION_POLICY"),
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
            config.store,
            StoreLimits(max_keys=1, max_bytes=4, eviction_policy=EvictionPolicy.REJECT),
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
                ServerConfig("127.0.0.1", 0, **kwargs)
