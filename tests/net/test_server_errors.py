import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from velis.codec import Identity, LengthPrefixedField, UInt
from velis.config import Server as ServerConfig
from velis.core.engine import Engine
from velis.net.protocol import WireProtocol
from velis.net.server import TcpServer


class ServerErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_error_reply_still_closes_and_disconnects(self) -> None:
        for max_frame, write_error in ((0, None), (1024, ConnectionResetError("gone"))):
            with self.subTest(max_frame=max_frame, write_error=write_error):
                engine = Mock(spec=Engine)
                wire = WireProtocol(
                    framing=LengthPrefixedField(Identity(), max_frame, prefix=UInt(4))
                )
                server = TcpServer(engine, ServerConfig("127.0.0.1", 0), wire=wire)
                reader = asyncio.StreamReader()
                reader.feed_data(b"\x00\x00\x00\x00")
                reader.feed_eof()
                writer = Mock(spec=asyncio.StreamWriter)
                writer.wait_closed = AsyncMock()
                writer.write.side_effect = write_error
                await server._handle_connection(reader, writer)
                engine.disconnect.assert_called_once()
                writer.close.assert_called_once()
                writer.wait_closed.assert_awaited_once()
