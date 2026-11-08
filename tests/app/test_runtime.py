import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests.helpers import ManualClock
from velis import __main__ as cli
from velis.app import _maintain, serve
from velis.config import Config, Server
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.store import Store
from velis.net.server import TcpServer


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_maintenance_drains_due_backlog_with_yields_between_rounds(
        self,
    ) -> None:
        clock = ManualClock()
        # Keep due entries until the maintenance task processes the backlog.
        store = Store(clock, eviction_policy="reject")
        for index in range(3001):
            key = str(index)
            store.set(key, b"v")
            store.expire(key, 0)
        snapshots: list[int] = []
        intervals = 0
        sleep = asyncio.sleep

        async def wait(delay: float) -> None:
            nonlocal intervals
            if delay:
                intervals += 1
                if intervals == 2:
                    raise asyncio.CancelledError
            else:
                snapshots.append(len(store))
                await sleep(0)

        with (
            patch("velis.app.asyncio.sleep", side_effect=wait),
            self.assertRaises(asyncio.CancelledError),
        ):
            await _maintain(Engine(store, Broker()), 60)
        self.assertEqual(snapshots, [2001, 1001, 1])
        self.assertEqual(len(store), 0)
        self.assertEqual(store.stored_bytes, 0)

    async def test_maintenance_continues_after_round_removing_only_stale_entries(
        self,
    ) -> None:
        clock = ManualClock()
        # Preserve stale entries for the maintenance continuation check.
        store = Store(clock, eviction_policy="reject")
        for index in range(1000):
            key = f"a{index}"
            store.set(key, b"v")
            store.expire(key, 0)
            store.set(key, b"live")
        store.set("z", b"due")
        store.expire("z", 0)
        intervals = 0
        yields = 0

        async def wait(delay: float) -> None:
            nonlocal intervals, yields
            if delay:
                intervals += 1
                if intervals == 2:
                    raise asyncio.CancelledError
            else:
                yields += 1

        with (
            patch("velis.app.asyncio.sleep", side_effect=wait),
            self.assertRaises(asyncio.CancelledError),
        ):
            await _maintain(Engine(store, Broker()), 60)
        self.assertEqual(yields, 1)
        self.assertEqual(len(store), 1000)
        self.assertFalse(store.has_due_expirations)

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
