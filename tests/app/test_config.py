import os
import unittest
from dataclasses import FrozenInstanceError
from unittest.mock import patch

from velis.config import Config, ServerLimits, SessionLimits, StoreLimits
from velis.core.eviction import EvictionPolicy


class ConfigTests(unittest.TestCase):
    def test_empty_mapping_uses_defaults_even_when_process_environment_is_set(
        self,
    ) -> None:
        with patch.dict(os.environ, {"VELIS_PORT": "1234"}):
            self.assertEqual(Config.from_env({}), Config())
        defaults = Config.from_env({})
        self.assertEqual(defaults.server_limits, ServerLimits())
        self.assertEqual(defaults.store_limits, StoreLimits())
        self.assertEqual(defaults.session_limits, SessionLimits())
        self.assertEqual(defaults.max_subscriptions, 1024)
        self.assertEqual(defaults.sweep_interval_ms, 100)
        self.assertEqual(defaults.log_level, "INFO")

    def test_environment_overrides_every_setting(self) -> None:
        source = {
            "VELIS_HOST": "localhost",
            "VELIS_PORT": "0",
            "VELIS_MAX_BUFFER": "16",
            "VELIS_MAX_CONNECTIONS": "2",
            "VELIS_MAX_KEYS": "3",
            "VELIS_MAX_BYTES": "32",
            "VELIS_EVICTION_POLICY": "lfu",
            "VELIS_MAX_SUBSCRIPTIONS": "4",
            "VELIS_SWEEP_MS": "7",
            "VELIS_LOG_LEVEL": "debug",
            "VELIS_MAX_PENDING_REQUESTS": "5",
            "VELIS_MAX_PENDING_MESSAGES": "6",
            "VELIS_MAX_PENDING_BYTES": "64",
        }
        self.assertEqual(
            Config.from_env(source),
            Config(
                server_limits=ServerLimits(
                    host="localhost", port=0, max_buffer=16, max_connections=2
                ),
                store_limits=StoreLimits(
                    max_keys=3, max_bytes=32, eviction_policy=EvictionPolicy.LFU
                ),
                session_limits=SessionLimits(
                    max_pending_requests=5, max_pending_messages=6, max_pending_bytes=64
                ),
                max_subscriptions=4,
                sweep_interval_ms=7,
                log_level="DEBUG",
            ),
        )

    def test_process_environment_is_read_when_loading_and_snapshots_are_immutable(
        self,
    ) -> None:
        with patch.dict(os.environ, {"VELIS_PORT": "1234"}, clear=True):
            first = Config.from_env()
            os.environ["VELIS_PORT"] = "5678"
            second = Config.from_env()
        self.assertEqual(first.server_limits.port, 1234)
        self.assertEqual(second.server_limits.port, 5678)
        self.assertEqual(Config().server_limits.port, 9000)
        for target, field, value in (
            (first, "log_level", "DEBUG"),
            (first.server_limits, "port", 1),
        ):
            with self.subTest(field=field), self.assertRaises(FrozenInstanceError):
                setattr(target, field, value)

    def test_malformed_numeric_settings_keep_variable_name_and_cause(self) -> None:
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
        ):
            for value in ("", "invalid", "1.5"):
                with self.subTest(name=name, value=value):
                    with self.assertRaisesRegex(ValueError, name) as raised:
                        Config.from_env({name: value})
                    self.assertIsInstance(raised.exception.__cause__, ValueError)

    def test_resource_limits_and_sweep_intervals_must_be_positive(self) -> None:
        for name in (
            "VELIS_MAX_BUFFER",
            "VELIS_MAX_CONNECTIONS",
            "VELIS_MAX_KEYS",
            "VELIS_MAX_BYTES",
            "VELIS_MAX_SUBSCRIPTIONS",
            "VELIS_SWEEP_MS",
            "VELIS_MAX_PENDING_REQUESTS",
            "VELIS_MAX_PENDING_MESSAGES",
            "VELIS_MAX_PENDING_BYTES",
        ):
            for value in ("0", "-1"):
                with (
                    self.subTest(name=name, value=value),
                    self.assertRaises(ValueError),
                ):
                    Config.from_env({name: value})

    def test_port_boundaries_allow_ephemeral_ports(self) -> None:
        for port in (0, 65535):
            with self.subTest(port=port):
                self.assertEqual(
                    Config.from_env({"VELIS_PORT": str(port)}).server_limits.port, port
                )
        for port in (-1, 65536):
            with self.subTest(port=port), self.assertRaisesRegex(ValueError, "port"):
                Config.from_env({"VELIS_PORT": str(port)})

    def test_every_policy_can_be_selected_from_the_environment(self) -> None:
        for policy in EvictionPolicy:
            with self.subTest(policy=policy):
                self.assertIs(
                    Config.from_env(
                        {"VELIS_EVICTION_POLICY": policy.value}
                    ).store_limits.eviction_policy,
                    policy,
                )

    def test_direct_runtime_settings_are_validated_too(self) -> None:
        self.assertEqual(Config(log_level="debug").log_level, "DEBUG")
        for build in (
            lambda: Config(sweep_interval_ms=0),
            lambda: Config(max_subscriptions=-1),
            lambda: Config(log_level="unknown"),
        ):
            with self.assertRaises(ValueError):
                build()
