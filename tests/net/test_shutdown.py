import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from velis.app import build_engine
from velis.config import ServerLimits, SessionLimits
from velis.net.server import TCPServer
from velis.net.session import TCPSession


class ShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_server_aborts_a_stalled_flush_after_the_grace_period(self) -> None:
        closed = asyncio.Event()
        listener = Mock(spec=asyncio.Server)
        listener.wait_closed = AsyncMock(side_effect=closed.wait)
        listener.abort_clients.side_effect = closed.set
        server = TCPServer(build_engine(), ServerLimits(close_timeout_seconds=0.01))
        server._listener = listener
        async with asyncio.timeout(1):
            await server.close()
        listener.close_clients.assert_called_once_with()
        listener.abort_clients.assert_called_once_with()
        self.assertIsNone(server._listener)

    async def test_cancelled_server_close_aborts_remaining_clients(self) -> None:
        waiting = asyncio.Event()
        blocked = asyncio.Event()

        async def wait_closed() -> None:
            waiting.set()
            await blocked.wait()

        listener = Mock(spec=asyncio.Server)
        listener.wait_closed = AsyncMock(side_effect=wait_closed)
        listener.abort_clients.side_effect = blocked.set
        server = TCPServer(build_engine(), ServerLimits())
        server._listener = listener
        task = asyncio.create_task(server.close())
        await waiting.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        listener.close.assert_called_once_with()
        listener.abort_clients.assert_called_once_with()

    async def test_session_timeout_aborts_without_cancelling_the_close_waiter(
        self,
    ) -> None:
        # StreamWriter reuses one close future. Cancelling it during a timeout
        # would make subsequent cleanup raise CancelledError even after abort.
        closed = asyncio.get_running_loop().create_future()
        writer = Mock(spec=asyncio.StreamWriter)

        async def wait_closed() -> None:
            await closed

        writer.wait_closed = AsyncMock(side_effect=wait_closed)
        writer.transport.abort.side_effect = lambda: closed.set_result(None)
        session = TCPSession(
            asyncio.StreamReader(),
            writer,
            limits=SessionLimits(close_timeout_seconds=0.01),
        )
        async with asyncio.timeout(1):
            await session.close()
        writer.transport.abort.assert_called_once_with()
        self.assertFalse(closed.cancelled())
        self.assertTrue(session._reader_task.done())

    async def test_cancelled_session_close_aborts_and_finishes_cleanup(self) -> None:
        waiting = asyncio.Event()
        closed = asyncio.Event()
        writer = Mock(spec=asyncio.StreamWriter)

        async def wait_closed() -> None:
            waiting.set()
            await closed.wait()

        writer.wait_closed = AsyncMock(side_effect=wait_closed)
        writer.transport.abort.side_effect = closed.set
        session = TCPSession(asyncio.StreamReader(), writer)
        task = asyncio.create_task(session.close())
        await waiting.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        writer.transport.abort.assert_called_once_with()
        self.assertTrue(session._reader_task.done())
