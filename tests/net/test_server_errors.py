import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from velis.codec import Identity, LengthPrefixedField, UInt
from velis.config import ServerConfig
from velis.core.engine import Engine
from velis.net.protocol import WireProtocol
from velis.net.server import TCPServer


class ServerErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_nested_context_preserves_the_running_listener(self) -> None:
        listener = Mock(spec=asyncio.Server)
        listener.wait_closed = AsyncMock()
        socket = Mock()
        socket.getsockname.return_value = ("127.0.0.1", 9000)
        listener.sockets = (socket,)
        server = TCPServer(Mock(spec=Engine), ServerConfig(port=0))
        with patch("velis.net.server.asyncio.start_server", return_value=listener):
            async with server:
                with self.assertRaisesRegex(RuntimeError, "already started"):
                    async with server:
                        self.fail("nested entry must be rejected")
                self.assertEqual(server.port, 9000)
                listener.close.assert_not_called()
        listener.close.assert_called_once()
        listener.close_clients.assert_called_once()
        listener.wait_closed.assert_awaited_once()

    async def test_failed_start_closes_a_partially_started_listener(self) -> None:
        listener = Mock(spec=asyncio.Server)
        listener.wait_closed = AsyncMock()
        listener.sockets = ()
        server = TCPServer(Mock(spec=Engine), ServerConfig(port=0))
        with (
            patch("velis.net.server.asyncio.start_server", return_value=listener),
            self.assertRaises(IndexError),
        ):
            async with server:
                self.fail("server must not enter after startup fails")
        self.assertIsNone(server._listener)
        listener.close.assert_called_once()
        listener.close_clients.assert_called_once()
        listener.wait_closed.assert_awaited_once()

    async def test_cancelled_handler_releases_admission_and_closes_connection(
        self,
    ) -> None:
        engine = Mock(spec=Engine)
        server = TCPServer(engine, ServerConfig(port=0))
        writer = Mock(spec=asyncio.StreamWriter)
        writer.wait_closed = AsyncMock()
        task = asyncio.create_task(
            server._handle_connection(asyncio.StreamReader(), writer)
        )
        await asyncio.sleep(0)
        self.assertEqual(server._connection_count, 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(server._connection_count, 0)
        engine.disconnect.assert_called_once()
        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()

    async def test_failed_error_reply_still_closes_and_disconnects(self) -> None:
        for max_frame, write_error in ((0, None), (1024, ConnectionResetError("gone"))):
            with self.subTest(max_frame=max_frame, write_error=write_error):
                engine = Mock(spec=Engine)
                wire = WireProtocol(
                    framing=LengthPrefixedField(Identity(), max_frame, prefix=UInt(4))
                )
                server = TCPServer(engine, ServerConfig("127.0.0.1", 0), wire=wire)
                reader = asyncio.StreamReader()
                reader.feed_data(b"\x00\x00\x00\x00")
                reader.feed_eof()
                writer = Mock(spec=asyncio.StreamWriter)
                writer.wait_closed = AsyncMock()
                writer.write.side_effect = write_error
                await server._handle_connection(reader, writer)
                self.assertEqual(server._connection_count, 0)
                engine.disconnect.assert_called_once()
                writer.close.assert_called_once()
                writer.wait_closed.assert_awaited_once()
