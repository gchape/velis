import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from velis.config import ServerConfig
from velis.core.engine import Engine
from velis.core.messages import Get, Ok
from velis.net.connection import Connection
from velis.net.server import COMMANDS_PER_TURN, TCPServer


class SchedulingTests(unittest.IsolatedAsyncioTestCase):
    async def test_ready_pipeline_yields_between_command_batches(self) -> None:
        total = COMMANDS_PER_TURN * 3 + 1

        async def commands():
            for _ in range(total):
                yield Get("key")

        engine = Mock(spec=Engine)
        engine.execute.return_value = Ok()
        connection = Mock(spec=Connection)
        connection.read_commands.return_value = commands()
        connection.drain = AsyncMock()
        server = TCPServer(engine, ServerConfig())
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
        self.assertEqual(connection.drain.await_count, total)
