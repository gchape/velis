import os
import unittest
from collections.abc import Callable
from typing import cast
from unittest.mock import patch

from velis.config import (
    Config,
    ServerLimits,
    SessionLimits,
    StoreLimits,
    normalize_log_level,
)
from velis.core.eviction import EvictionPolicy


class ConfigTests(unittest.TestCase):
    def test_counts_and_budgets_require_integers(self) -> None:
        cases = (
            (ServerLimits, ("port", "max_buffer", "max_connections")),
            (StoreLimits, ("max_keys", "max_bytes")),
            (
                SessionLimits,
                (
                    "max_pending_requests",
                    "max_pending_messages",
                    "max_pending_bytes",
                    "max_batch_bytes",
                ),
            ),
            (Config, ("max_subscriptions", "sweep_interval_ms")),
        )
        for constructor, names in cases:
            for name in names:
                for value in (1.5, True, "2", float("nan"), float("inf")):
                    with (
                        self.subTest(name=name, value=value),
                        self.assertRaises(ValueError),
                    ):
                        cast(Callable[..., object], constructor)(**{name: value})

    def test_config_reexports_component_limits(self) -> None:
        from velis.core import store
        from velis.net import server, session

        self.assertIs(StoreLimits, store.StoreLimits)
        self.assertIs(ServerLimits, server.ServerLimits)
        self.assertIs(SessionLimits, session.SessionLimits)

    def test_empty_mapping_uses_defaults_without_reading_process_environment(
        self,
    ) -> None:
        with patch.dict(os.environ, {"VELIS_PORT": "invalid"}):
            self.assertEqual(Config.from_env({}), Config())
            with self.assertRaisesRegex(ValueError, "VELIS_PORT"):
                Config.from_env()

    def test_all_environment_settings_are_loaded(self) -> None:
        environ = {
            "VELIS_HOST": "localhost",
            "VELIS_PORT": "0",
            "VELIS_MAX_BUFFER": "100",
            "VELIS_MAX_CONNECTIONS": "2",
            "VELIS_MAX_KEYS": "3",
            "VELIS_MAX_BYTES": "200",
            "VELIS_EVICTION_POLICY": "lfu",
            "VELIS_MAX_SUBSCRIPTIONS": "4",
            "VELIS_SWEEP_MS": "5",
            "VELIS_LOG_LEVEL": "debug",
            "VELIS_MAX_PENDING_REQUESTS": "6",
            "VELIS_MAX_PENDING_MESSAGES": "7",
            "VELIS_MAX_PENDING_BYTES": "300",
            "VELIS_MAX_BATCH_BYTES": "400",
            "VELIS_CLOSE_TIMEOUT_SECONDS": "2.5",
        }
        before = environ.copy()
        config = Config.from_env(environ)
        self.assertEqual(
            config,
            Config(
                server_limits=ServerLimits("localhost", 0, 100, 2, 2.5),
                store_limits=StoreLimits(3, 200, EvictionPolicy.LFU),
                session_limits=SessionLimits(6, 7, 300, 400, 2.5),
                max_subscriptions=4,
                sweep_interval_ms=5,
                log_level="DEBUG",
            ),
        )
        self.assertEqual(environ, before)

    def test_numeric_parse_errors_name_the_environment_variable(self) -> None:
        for name in (
            "VELIS_PORT",
            "VELIS_MAX_BUFFER",
            "VELIS_MAX_CONNECTIONS",
            "VELIS_MAX_KEYS",
            "VELIS_MAX_BYTES",
            "VELIS_MAX_SUBSCRIPTIONS",
            "VELIS_SWEEP_MS",
            "VELIS_MAX_PENDING_REQUESTS",
            "VELIS_MAX_PENDING_MESSAGES",
            "VELIS_MAX_PENDING_BYTES",
            "VELIS_MAX_BATCH_BYTES",
        ):
            for value in ("", "abc", "1.5"):
                with (
                    self.subTest(name=name, value=value),
                    self.assertRaisesRegex(ValueError, name),
                ):
                    Config.from_env({name: value})

    def test_close_timeout_rejects_nonpositive_and_nonfinite_values(self) -> None:
        for value in (0, -1, float("nan"), float("inf")):
            for limits in (ServerLimits, SessionLimits):
                with (
                    self.subTest(value=value, limits=limits),
                    self.assertRaisesRegex(ValueError, "close_timeout_seconds"),
                ):
                    limits(close_timeout_seconds=value)
            with self.assertRaises(ValueError):
                Config.from_env({"VELIS_CLOSE_TIMEOUT_SECONDS": str(value)})
        with self.assertRaisesRegex(ValueError, "VELIS_CLOSE_TIMEOUT_SECONDS"):
            Config.from_env({"VELIS_CLOSE_TIMEOUT_SECONDS": "abc"})

    def test_invalid_named_settings_do_not_silently_fall_back(self) -> None:
        for name in ("VELIS_EVICTION_POLICY", "VELIS_LOG_LEVEL"):
            for value in ("", "unknown"):
                with (
                    self.subTest(name=name, value=value),
                    self.assertRaisesRegex(ValueError, name),
                ):
                    Config.from_env({name: value})

    def test_nonpositive_limits_are_rejected(self) -> None:
        cases: tuple[tuple[str, Callable[[int], object]], ...] = (
            ("max_buffer", lambda value: ServerLimits(max_buffer=value)),
            ("max_connections", lambda value: ServerLimits(max_connections=value)),
            ("max_keys", lambda value: StoreLimits(max_keys=value)),
            ("max_bytes", lambda value: StoreLimits(max_bytes=value)),
            (
                "max_pending_requests",
                lambda value: SessionLimits(max_pending_requests=value),
            ),
            (
                "max_pending_messages",
                lambda value: SessionLimits(max_pending_messages=value),
            ),
            ("max_pending_bytes", lambda value: SessionLimits(max_pending_bytes=value)),
            ("max_batch_bytes", lambda value: SessionLimits(max_batch_bytes=value)),
            ("max_subscriptions", lambda value: Config(max_subscriptions=value)),
            ("sweep_interval_ms", lambda value: Config(sweep_interval_ms=value)),
        )
        for field, construct in cases:
            for value in (0, -1):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaisesRegex(ValueError, field),
                ):
                    construct(value)

    def test_port_boundaries_include_ephemeral_port_zero(self) -> None:
        for port in (0, 65535):
            self.assertEqual(ServerLimits(port=port).port, port)
        for port in (-1, 65536):
            with self.assertRaisesRegex(ValueError, "invalid port"):
                ServerLimits(port=port)

    def test_log_levels_are_case_insensitive_and_unknown_levels_fail(self) -> None:
        for level in ("debug", "INFO", "wArNiNg", "error", "critical", "notset"):
            self.assertEqual(normalize_log_level(level), level.upper())
            self.assertEqual(Config(log_level=level).log_level, level.upper())
        for level in ("", "20", "verbose"):
            with self.assertRaisesRegex(ValueError, "invalid log level"):
                normalize_log_level(level)

    def test_policy_strings_are_normalized_and_invalid_policies_fail(self) -> None:
        for policy in EvictionPolicy:
            config = Config.from_env({"VELIS_EVICTION_POLICY": policy.value})
            self.assertIs(config.store_limits.eviction_policy, policy)
