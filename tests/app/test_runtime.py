import asyncio
import unittest
from unittest.mock import Mock, patch

from tests.helpers import ManualClock, RecordingSubscriber
from velis.app import _maintain, build_engine, serve
from velis.config import ServerLimits, StoreLimits
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.messages import Read, Value, Watch, Write
from velis.core.store import Store
from velis.errors import CapacityError, CommandError
from velis.net.client import Client


class BuildEngineTests(unittest.TestCase):
    def test_build_engine_applies_limits_and_creates_independent_state(self) -> None:
        engine = build_engine(
            store_limits=StoreLimits(max_bytes=2), max_subscriptions=1
        )
        subscriber = RecordingSubscriber()
        engine.execute(Write("k", b"v"), subscriber)
        with self.assertRaises(CapacityError):
            engine.execute(Write("k", b"xx"), subscriber)
        engine.execute(Watch("a"), subscriber)
        with self.assertRaises(CapacityError):
            engine.execute(Watch("b"), subscriber)
        self.assertNotEqual(
            engine.execute(Read("k"), subscriber),
            build_engine().execute(Read("k"), subscriber),
        )


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_serve_owns_listener_and_closes_connected_clients(self) -> None:
        async with serve(ServerLimits(port=0)) as server:
            client = await Client.connect("127.0.0.1", server.port)
            self.addAsyncCleanup(client.close)
            await client.write("k", b"v")
            self.assertEqual(await client.read("k"), b"v")
        with self.assertRaisesRegex(RuntimeError, "not started"):
            _ = server.port
        with self.assertRaises(ConnectionError):
            async with asyncio.timeout(5):
                await client.next_message()

    async def test_injected_engine_is_reused_with_its_original_limits(self) -> None:
        engine = build_engine(store_limits=StoreLimits(max_bytes=2))
        async with (
            serve(
                ServerLimits(port=0),
                engine=engine,
                store_limits=StoreLimits(max_bytes=100),
            ) as server,
            await Client.connect("127.0.0.1", server.port) as client,
        ):
            await client.write("k", b"v")
            with self.assertRaisesRegex(CommandError, "byte limit"):
                await client.write("k", b"xx")
        self.assertEqual(engine.execute(Read("k"), RecordingSubscriber()), Value(b"v"))

    async def test_background_maintenance_removes_expired_keys_without_reads(
        self,
    ) -> None:
        clock = ManualClock()
        store = Store(clock)
        store.set("k", b"v")
        store.expire("k", 5)
        engine = Engine(store, Broker())
        swept = asyncio.Event()
        original = engine.sweep

        def sweep() -> int:
            removed = original()
            if removed:
                swept.set()
            return removed

        with patch.object(engine, "sweep", side_effect=sweep):
            async with serve(ServerLimits(port=0), engine=engine, sweep_interval_ms=1):
                clock.advance(5)
                async with asyncio.timeout(5):
                    await swept.wait()
                self.assertEqual(len(store), 0)
                self.assertEqual(store.stats().expirations, 1)

    async def test_maintenance_yields_between_due_batches_before_sleeping_again(
        self,
    ) -> None:
        engine = Mock(spec=Engine)
        engine.has_due_expirations = True
        intervals: list[float] = []

        def sweep() -> None:
            if engine.sweep.call_count == 3:
                engine.has_due_expirations = False

        async def sleep(interval: float) -> None:
            intervals.append(interval)
            if len(intervals) == 4:
                raise asyncio.CancelledError

        engine.sweep.side_effect = sweep
        with (
            patch("velis.app.asyncio.sleep", side_effect=sleep),
            self.assertRaises(asyncio.CancelledError),
        ):
            await _maintain(engine, 0.1)
        self.assertEqual(intervals, [0.1, 0, 0, 0.1])
        self.assertEqual(engine.sweep.call_count, 3)

    async def test_maintenance_failure_interrupts_caller_and_closes_server(
        self,
    ) -> None:
        engine = build_engine()
        error = RuntimeError("sweep failed")
        with (
            patch.object(engine, "sweep", side_effect=error),
            self.assertRaises(ExceptionGroup) as caught,
        ):
            async with serve(
                ServerLimits(port=0), engine=engine, sweep_interval_ms=1
            ) as server:
                async with asyncio.timeout(5):
                    await asyncio.Future()
        self.assertIn(error, caught.exception.exceptions)
        with self.assertRaisesRegex(RuntimeError, "not started"):
            _ = server.port

    async def test_caller_failure_closes_runtime_and_propagates(self) -> None:
        error = ValueError("caller failed")
        with self.assertRaises(ExceptionGroup) as caught:
            async with serve(ServerLimits(port=0)) as server:
                raise error
        self.assertIn(error, caught.exception.exceptions)
        with self.assertRaises(RuntimeError):
            _ = server.port

    async def test_invalid_sweep_interval_fails_before_opening_resources(self) -> None:
        for interval in (0, -1):
            with (
                self.subTest(interval=interval),
                patch("velis.app.TCPServer") as server,
            ):
                with self.assertRaisesRegex(ValueError, "sweep_interval_ms"):
                    async with serve(sweep_interval_ms=interval):
                        self.fail("invalid runtime started")
                server.assert_not_called()
