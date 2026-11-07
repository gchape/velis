import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from velis import __main__ as cli
from velis.app import serve
from velis.config import Config, Server
from velis.core.engine import Engine
from velis.net.server import TcpServer


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = Config("INFO", Server("127.0.0.1", 0))
        self.server = Mock(spec=TcpServer)
        self.server.start = AsyncMock()
        self.server.close = AsyncMock()
        self.patch_server = patch("velis.app.TcpServer", return_value=self.server)
        self.patch_server.start()
        self.addCleanup(self.patch_server.stop)

    async def test_runtime_stops_maintenance_before_closing_server(self) -> None:
        started, stopped = asyncio.Event(), asyncio.Event()

        async def maintain(engine, interval):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        async def close():
            self.assertTrue(stopped.is_set())

        self.server.close.side_effect = close
        with patch("velis.app._maintain", side_effect=maintain):
            async with asyncio.timeout(1):
                async with serve(self.config) as server:
                    self.assertIs(server, self.server)
                    await started.wait()
                    self.server.start.assert_awaited_once()
            self.server.close.assert_awaited_once()

    async def test_startup_failure_still_closes_server(self) -> None:
        self.server.start.side_effect = OSError("bind failed")
        with patch("velis.app._maintain") as maintain:
            with self.assertRaisesRegex(OSError, "bind failed"):
                async with serve(self.config):
                    self.fail("runtime should not start")
            maintain.assert_not_called()
        self.server.close.assert_awaited_once()

    async def test_caller_failure_still_closes_server(self) -> None:
        failure = RuntimeError("caller failed")
        with self.assertRaises(ExceptionGroup) as raised:
            async with serve(self.config):
                raise failure
        self.assertEqual(raised.exception.exceptions, (failure,))
        self.server.close.assert_awaited_once()

    async def test_maintenance_failure_interrupts_caller_and_closes(self) -> None:
        """A real sweep failure reaches the caller while the runtime is active."""
        failure = RuntimeError("maintenance failed")
        engine = Mock(spec=Engine)
        engine.sweep.side_effect = failure
        config = Config("INFO", self.config.server, sweep_interval_ms=1)
        interrupted = asyncio.Event()
        async with asyncio.timeout(1):
            with self.assertRaises(ExceptionGroup) as raised:
                async with serve(config, engine=engine):
                    try:
                        await asyncio.Event().wait()
                    finally:
                        interrupted.set()
        self.assertEqual(raised.exception.exceptions, (failure,))
        self.assertTrue(interrupted.is_set())
        engine.sweep.assert_called_once()
        self.server.close.assert_awaited_once()

    async def test_cancellation_stops_workers_and_closes_server(self) -> None:
        """External cancellation retains its meaning and awaits worker cleanup."""
        started, stopped = asyncio.Event(), asyncio.Event()

        async def maintain(engine, interval):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        async def run():
            async with serve(self.config):
                await asyncio.Event().wait()

        with patch("velis.app._maintain", side_effect=maintain):
            async with asyncio.timeout(1):
                task = asyncio.create_task(run())
                try:
                    await started.wait()
                finally:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
        self.assertTrue(stopped.is_set())
        self.server.close.assert_awaited_once()

    async def test_signal_setup_failure_restores_handlers_before_startup(self) -> None:
        loop = asyncio.get_running_loop()
        with (
            patch.object(
                loop, "add_signal_handler", side_effect=[None, RuntimeError("signal")]
            ),
            patch.object(loop, "remove_signal_handler") as remove,
            patch("velis.__main__.serve") as runtime,
        ):
            with self.assertRaisesRegex(RuntimeError, "signal"):
                await cli._main(self.config)
            remove.assert_called_once()
            runtime.assert_not_called()
