import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from velis.core.engine import Engine
from velis.core.messages import Get, Ok
from velis.net.connection import Connection
from velis.net.server import COMMANDS_PER_TURN, ServerLimits, TCPServer


class SchedulingTests(unittest.IsolatedAsyncioTestCase):
    async def test_command_failure_closes_the_incoming_generator(self) -> None:
        closed = asyncio.Event()

        async def commands():
            try:
                yield Get("key")
            finally:
                closed.set()

        failure = RuntimeError("command failed")
        engine = Mock(spec=Engine)
        engine.execute.side_effect = failure
        connection = Mock(spec=Connection)
        connection.read_commands.return_value = commands()
        server = TCPServer(engine, ServerLimits())
        with self.assertRaises(RuntimeError) as raised:
            await server._serve_commands(connection)
        self.assertIs(raised.exception, failure)
        self.assertTrue(closed.is_set())

    async def test_ready_pipeline_yields_between_command_batches(self) -> None:
        total = COMMANDS_PER_TURN * 3 + 1

        async def commands():
            for _ in range(total):
                yield Get("key")

        engine = Mock(spec=Engine)
        engine.execute.return_value = Ok()
        connection = Mock(spec=Connection)
        connection.read_commands.return_value = commands()
        connection.write_reply = AsyncMock()
        server = TCPServer(engine, ServerLimits())
        progress = []

        async def observe() -> None:
            while connection.write_reply.call_count < total:
                await asyncio.sleep(0)
                progress.append(connection.write_reply.call_count)

        observer = asyncio.create_task(observe())
        await asyncio.sleep(0)
        async with asyncio.timeout(1):
            await server._serve_commands(connection)
            await observer
        self.assertEqual(
            progress,
            [COMMANDS_PER_TURN, COMMANDS_PER_TURN * 2, COMMANDS_PER_TURN * 3, total],
        )
        self.assertEqual(engine.execute.call_count, total)
        self.assertEqual(connection.write_reply.await_count, total)
