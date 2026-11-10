import signal
import unittest
from contextlib import asynccontextmanager
from unittest.mock import Mock, patch, sentinel

from velis import __main__ as cli
from velis.config import Config, ServerLimits


class CLITests(unittest.TestCase):
    def test_logging_configuration_normalizes_level_before_configuring(self) -> None:
        with patch("velis.__main__.logging.config.dictConfig") as configure:
            cli.configure_logging("debug")
        config = configure.call_args.args[0]
        self.assertEqual(config["loggers"]["velis"]["level"], "DEBUG")
        self.assertFalse(config["disable_existing_loggers"])

    def test_invalid_log_level_does_not_change_logging(self) -> None:
        with (
            patch("velis.__main__.logging.config.dictConfig") as configure,
            self.assertRaises(ValueError),
        ):
            cli.configure_logging("invalid")
        configure.assert_not_called()

    def test_main_passes_validated_configuration_to_runtime(self) -> None:
        config = Config(
            server_limits=ServerLimits(port=0),
            max_subscriptions=3,
            sweep_interval_ms=5,
            log_level="debug",
        )
        with (
            patch("velis.__main__.Config.from_env", return_value=config),
            patch("velis.__main__.configure_logging") as logging,
            patch("velis.__main__.serve", return_value=sentinel.runtime) as serve,
            patch(
                "velis.__main__.asyncio.run",
                side_effect=lambda coroutine: coroutine.close(),
            ) as run,
        ):
            cli.main()
        logging.assert_called_once_with("DEBUG")
        serve.assert_called_once_with(
            config.server_limits,
            store_limits=config.store_limits,
            max_subscriptions=3,
            sweep_interval_ms=5,
        )
        run.assert_called_once()

    def test_invalid_environment_stops_before_logging_or_runtime_creation(self) -> None:
        with (
            patch(
                "velis.__main__.Config.from_env",
                side_effect=ValueError("invalid config"),
            ),
            patch("velis.__main__.configure_logging") as logging,
            patch("velis.__main__.serve") as serve,
            self.assertRaises(ValueError),
        ):
            cli.main()
        logging.assert_not_called()
        serve.assert_not_called()


class CLISignalTests(unittest.IsolatedAsyncioTestCase):
    async def test_signal_stops_runtime_and_removes_installed_handlers(self) -> None:
        loop = Mock()
        callbacks = {}
        loop.add_signal_handler.side_effect = lambda sig, callback: (
            callbacks.setdefault(sig, callback)
        )
        exited = False

        @asynccontextmanager
        async def runtime():
            nonlocal exited
            callbacks[signal.SIGTERM]()
            try:
                yield sentinel.server
            finally:
                exited = True

        with patch("velis.__main__.asyncio.get_running_loop", return_value=loop):
            await cli._main(runtime())
        self.assertTrue(exited)
        self.assertEqual(set(callbacks), {signal.SIGINT, signal.SIGTERM})
        self.assertEqual(
            {call.args[0] for call in loop.remove_signal_handler.call_args_list},
            {signal.SIGINT, signal.SIGTERM},
        )

    async def test_partial_signal_installation_is_cleaned_up_on_failure(self) -> None:
        loop = Mock()
        loop.add_signal_handler.side_effect = [None, RuntimeError("signal failed")]
        with (
            patch("velis.__main__.asyncio.get_running_loop", return_value=loop),
            self.assertRaisesRegex(RuntimeError, "signal failed"),
        ):
            await cli._main(sentinel.runtime)
        loop.remove_signal_handler.assert_called_once_with(signal.SIGINT)
