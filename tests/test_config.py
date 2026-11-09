import os
import unittest
from dataclasses import dataclass
from unittest.mock import patch

from tests.helpers import RecordingSubscriber
from velis.app import build_engine
from velis.config import Config, ServerConfig
from velis.core.eviction import EvictionPolicy
from velis.core.messages import Get, Nil, Set, Subscribe, Value
from velis.core.store import StoreLimits
from velis.errors import CapacityError


class ConfigTests(unittest.TestCase):
    def test_unset_environment_preserves_dataclass_defaults(self) -> None:
        @dataclass(frozen=True, slots=True)
        class CustomServer(ServerConfig):
            port: int = 12345
            max_connections: int = 5

        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(Config.from_env(), Config())
            self.assertEqual(CustomServer.from_env(), CustomServer())
        with patch.dict(os.environ, {"VELIS_PORT": "0"}, clear=True):
            self.assertEqual(CustomServer.from_env(), CustomServer(port=0))

    def test_log_level_normalizes_for_direct_and_environment_construction(self) -> None:
        self.assertEqual(Config(log_level="info").log_level, "INFO")
        with patch.dict(os.environ, {"VELIS_LOG_LEVEL": "debug"}, clear=True):
            self.assertEqual(Config.from_env().log_level, "DEBUG")
        with self.assertRaisesRegex(ValueError, "invalid log level"):
            Config(log_level="unknown")

    def test_invalid_environment_integer_keeps_variable_and_cause(self) -> None:
        with (
            patch.dict(os.environ, {"VELIS_MAX_KEYS": "invalid"}, clear=True),
            self.assertRaisesRegex(ValueError, "VELIS_MAX_KEYS") as raised,
        ):
            Config.from_env()
        self.assertIsInstance(raised.exception.__cause__, ValueError)

    def test_default_environment_policy_is_s3fifo(self) -> None:
        with patch.dict(
            os.environ,
            {"VELIS_MAX_KEYS": "1"},
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual(config.store.eviction_policy, EvictionPolicy.S3_FIFO)
        self.assertEqual(StoreLimits().eviction_policy, EvictionPolicy.S3_FIFO)
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

    def test_lfu_environment_policy_reaches_the_engine(self) -> None:
        with patch.dict(
            os.environ,
            {"VELIS_EVICTION_POLICY": "lfu", "VELIS_MAX_KEYS": "2"},
            clear=True,
        ):
            config = Config.from_env()
        self.assertIs(config.store.eviction_policy, EvictionPolicy.LFU)
        engine = build_engine(config)
        subscriber = RecordingSubscriber()
        for key in ("a", "b"):
            engine.execute(Set(key, key.encode()), subscriber)
        self.assertEqual(engine.execute(Get("a"), subscriber), Value(b"a"))
        engine.execute(Set("c", b"c"), subscriber)
        self.assertEqual(engine.execute(Get("b"), subscriber), Nil())

    def test_removed_policies_fail_with_the_environment_variable_name(self) -> None:
        for policy in ("fifo", "random", "reject", "sieve", "rr", "invalid"):
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
                "VELIS_EVICTION_POLICY": "s3-fifo",
                "VELIS_MAX_CONNECTIONS": "2",
                "VELIS_MAX_SUBSCRIPTIONS": "1",
            },
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual(
            config.store,
            StoreLimits(
                max_keys=1, max_bytes=4, eviction_policy=EvictionPolicy.S3_FIFO
            ),
        )
        self.assertEqual(config.server.max_connections, 2)
        engine = build_engine(config)
        subscriber = RecordingSubscriber()
        engine.execute(Set("a", b"123"), subscriber)
        engine.execute(Set("b", b""), subscriber)
        self.assertEqual(engine.execute(Get("a"), subscriber), Nil())
        self.assertEqual(engine.execute(Get("b"), subscriber), Value(b""))
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
