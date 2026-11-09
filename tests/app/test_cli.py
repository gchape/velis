import os
import unittest
from unittest.mock import Mock, patch

from tests.helpers import RecordingSubscriber
from velis import __main__ as cli
from velis.app import build_engine
from velis.config import (
    DEFAULT_MAX_SUBSCRIPTIONS,
    DEFAULT_SERVER_LIMITS,
    DEFAULT_STORE_LIMITS,
    DEFAULT_SWEEP_INTERVAL_MS,
    ServerLimits,
    StoreLimits,
)
from velis.core.engine import Engine
from velis.core.eviction import EvictionPolicy
from velis.core.messages import Get, Nil, Set, Subscribe, Value
from velis.errors import CapacityError


class CLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.run_looptime = object()
        self.coroutine = object()
        self.serve = Mock(return_value=self.run_looptime)
        self.main = Mock(return_value=self.coroutine)
        self.run_loop = Mock()
        self.logging = Mock()
        for patcher in (
            patch.dict(os.environ, {}, clear=True),
            patch.object(cli, "serve", self.serve),
            patch.object(cli, "_main", self.main),
            patch.object(cli.asyncio, "run", self.run_loop),
            patch.object(cli.logging.config, "dictConfig", self.logging),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def _engine(self) -> Engine:
        cli.main()
        options = self.serve.call_args.kwargs
        return build_engine(
            store_limits=options["store_limits"],
            max_subscriptions=options["max_subscriptions"],
        )

    def test_unset_environment_preserves_component_defaults(self) -> None:
        cli.main()
        self.serve.assert_called_once_with(
            DEFAULT_SERVER_LIMITS,
            store_limits=DEFAULT_STORE_LIMITS,
            max_subscriptions=DEFAULT_MAX_SUBSCRIPTIONS,
            sweep_interval_ms=DEFAULT_SWEEP_INTERVAL_MS,
        )
        self.main.assert_called_once_with(self.run_looptime)
        self.run_loop.assert_called_once_with(self.coroutine)
        self.assertEqual(
            self.logging.call_args.args[0]["loggers"]["velis"]["level"], "INFO"
        )

    def test_log_level_normalizes_before_logging_setup(self) -> None:
        cli.configure_logging("info")
        self.assertEqual(
            self.logging.call_args.args[0]["loggers"]["velis"]["level"], "INFO"
        )
        with patch.dict(os.environ, {"VELIS_LOG_LEVEL": "debug"}):
            cli.main()
        self.assertEqual(
            self.logging.call_args.args[0]["loggers"]["velis"]["level"], "DEBUG"
        )

    def test_invalid_log_level_fails_before_changing_logging_or_starting(self) -> None:
        for level in ("", "unknown"):
            with (
                self.subTest(level=level),
                patch.dict(os.environ, {"VELIS_LOG_LEVEL": level}),
                self.assertRaisesRegex(ValueError, "invalid log level"),
            ):
                cli.main()
        self.logging.assert_not_called()
        self.run_loop.assert_not_called()

    def test_invalid_environment_integer_keeps_variable_and_cause(self) -> None:
        for name in ("VELIS_MAX_KEYS", "VELIS_SWEEP_MS"):
            with (
                self.subTest(name=name),
                patch.dict(os.environ, {name: "invalid"}),
                self.assertRaisesRegex(ValueError, name) as raised,
            ):
                cli.main()
            self.assertIsInstance(raised.exception.__cause__, ValueError)

    def test_default_environment_policy_is_s3fifo(self) -> None:
        with patch.dict(os.environ, {"VELIS_MAX_KEYS": "1"}):
            engine = self._engine()
        self.assertIs(
            self.serve.call_args.kwargs["store_limits"].eviction_policy,
            EvictionPolicy.S3_FIFO,
        )
        subscriber = RecordingSubscriber()
        engine.execute(Set("a", b"first"), subscriber)
        engine.execute(Set("b", b"second"), subscriber)
        self.assertEqual(engine.execute(Get("a"), subscriber), Nil())
        self.assertEqual(engine.execute(Get("b"), subscriber), Value(b"second"))

    def test_invalid_eviction_policy_fails_at_startup(self) -> None:
        for policy in ("", "invalid", "LRU", " lru"):
            with (
                self.subTest(policy=policy),
                patch.dict(os.environ, {"VELIS_EVICTION_POLICY": policy}),
                self.assertRaisesRegex(ValueError, "VELIS_EVICTION_POLICY"),
            ):
                cli.main()

    def test_lfu_environment_policy_reaches_the_engine(self) -> None:
        with patch.dict(
            os.environ, {"VELIS_EVICTION_POLICY": "lfu", "VELIS_MAX_KEYS": "2"}
        ):
            engine = self._engine()
        subscriber = RecordingSubscriber()
        for key in ("a", "b"):
            engine.execute(Set(key, key.encode()), subscriber)
        engine.execute(Get("a"), subscriber)
        engine.execute(Set("c", b"c"), subscriber)
        self.assertEqual(engine.execute(Get("b"), subscriber), Nil())

    def test_environment_limits_reach_runtime_and_engine(self) -> None:
        with patch.dict(
            os.environ,
            {
                "VELIS_HOST": "localhost",
                "VELIS_PORT": "0",
                "VELIS_MAX_BUFFER": "16",
                "VELIS_MAX_CONNECTIONS": "2",
                "VELIS_MAX_KEYS": "1",
                "VELIS_MAX_BYTES": "4",
                "VELIS_EVICTION_POLICY": "s3-fifo",
                "VELIS_MAX_SUBSCRIPTIONS": "1",
                "VELIS_SWEEP_MS": "7",
            },
        ):
            engine = self._engine()
        self.serve.assert_called_once_with(
            ServerLimits(host="localhost", port=0, max_buffer=16, max_connections=2),
            store_limits=StoreLimits(max_keys=1, max_bytes=4),
            max_subscriptions=1,
            sweep_interval_ms=7,
        )
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
            "VELIS_MAX_BUFFER",
            "VELIS_MAX_CONNECTIONS",
            "VELIS_MAX_SUBSCRIPTIONS",
            "VELIS_SWEEP_MS",
            "VELIS_MAX_PENDING_REQUESTS",
            "VELIS_MAX_PENDING_MESSAGES",
            "VELIS_MAX_PENDING_BYTES",
        ):
            for value in ("0", "-1", "invalid"):
                with (
                    self.subTest(name=name, value=value),
                    patch.dict(os.environ, {name: value}),
                    self.assertRaises(ValueError),
                ):
                    cli.main()
        self.run_loop.assert_not_called()
        self.logging.assert_not_called()
        self.serve.assert_not_called()

    def test_environment_rejects_invalid_ports(self) -> None:
        for value in ("-1", "65536", "invalid"):
            with (
                self.subTest(value=value),
                patch.dict(os.environ, {"VELIS_PORT": value}),
                self.assertRaises(ValueError),
            ):
                cli.main()
        self.run_loop.assert_not_called()
