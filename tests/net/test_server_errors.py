import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from velis.core.engine import Engine
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.server import ServerLimits, TCPServer


class ServerErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_port_requires_a_running_listener(self) -> None:
        server = TCPServer(Mock(spec=Engine), ServerLimits(port=0))
        with self.assertRaisesRegex(RuntimeError, "not started"):
            _ = server.port
        await server.close()
        with self.assertRaisesRegex(RuntimeError, "not started"):
            _ = server.port

    async def test_nested_context_preserves_the_running_listener(self) -> None:
        listener = Mock(spec=asyncio.Server)
        listener.wait_closed = AsyncMock()
        socket = Mock()
        socket.getsockname.return_value = ("127.0.0.1", 9000)
        listener.sockets = (socket,)
        server = TCPServer(Mock(spec=Engine), ServerLimits(port=0))
        with patch.object(
            asyncio.get_running_loop(), "create_server", return_value=listener
        ):
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
        server = TCPServer(Mock(spec=Engine), ServerLimits(port=0))
        with (
            patch.object(
                asyncio.get_running_loop(), "create_server", return_value=listener
            ),
            self.assertRaises(IndexError),
        ):
            async with server:
                self.fail("server must not enter after startup fails")
        self.assertIsNone(server._listener)
        listener.close.assert_called_once()
        listener.close_clients.assert_called_once()
        listener.wait_closed.assert_awaited_once()

    async def test_connection_lost_releases_admission_once(self) -> None:
        engine = Mock(spec=Engine)
        server = TCPServer(engine, ServerLimits(port=0))
        transport = Mock(spec=asyncio.Transport)
        transport.is_closing.return_value = False
        connection = server._new_connection()
        connection.connection_made(transport)
        self.assertEqual(server._connection_count, 1)
        connection.connection_lost(None)
        connection.connection_lost(None)
        self.assertEqual(server._connection_count, 0)
        engine.disconnect.assert_called_once_with(connection)

    async def test_failed_error_reply_still_closes_and_disconnects(self) -> None:
        for encode_error, write_error in (
            (ProtocolError("cannot encode"), None),
            (None, ConnectionResetError("gone")),
        ):
            with self.subTest(encode_error=encode_error, write_error=write_error):
                engine = Mock(spec=Engine)
                server = TCPServer(engine, ServerLimits("127.0.0.1", 0))
                transport = Mock(spec=asyncio.Transport)
                transport.is_closing.return_value = False
                transport.write.side_effect = write_error
                connection = server._new_connection()
                connection.connection_made(transport)
                with patch.object(
                    protocol,
                    "encode_message",
                    side_effect=encode_error,
                    wraps=protocol.encode_message,
                ):
                    connection.data_received(b"\x00\x00\x00\x00")
                transport.close.assert_called_once()
                connection.connection_lost(None)
                self.assertEqual(server._connection_count, 0)
                engine.disconnect.assert_called_once_with(connection)
