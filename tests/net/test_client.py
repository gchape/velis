import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from velis.codec import CodecChain
from velis.net.client import Client


class ClientLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.reader = asyncio.StreamReader()
        self.writer = Mock(spec=asyncio.StreamWriter)
        self.writer.drain = AsyncMock()
        self.writer.wait_closed = AsyncMock()
        self.client = Client(self.reader, self.writer, CodecChain())
        self.addAsyncCleanup(self.client.close)

    async def test_close_wakes_message_reader(self) -> None:
        pending = asyncio.create_task(self.client.next_message())
        await asyncio.sleep(0)
        await self.client.close()
        async with asyncio.timeout(1):
            with self.assertRaises(ConnectionError):
                await pending
            with self.assertRaises(ConnectionError):
                await self.client.next_message()

    async def test_close_wakes_pending_request(self) -> None:
        pending = asyncio.create_task(self.client.get("key"))
        await asyncio.sleep(0)
        await self.client.close()
        async with asyncio.timeout(1):
            with self.assertRaises(ConnectionError):
                await pending
            with self.assertRaises(ConnectionError):
                await self.client.get("another")

    async def test_cancelled_request_closes_connection(self) -> None:
        pending = asyncio.create_task(self.client.get("key"))
        await asyncio.sleep(0)
        self.writer.write.assert_called_once()
        pending.cancel()
        async with asyncio.timeout(1):
            with self.assertRaises(asyncio.CancelledError):
                await pending
            with self.assertRaises(ConnectionError):
                await self.client.get("another")
        self.writer.close.assert_called_once()

    async def test_malformed_frame_closes_stream_and_wakes_readers(self) -> None:
        self.reader.feed_data(b"\x00\x00\x00\x04NOPE")
        async with asyncio.timeout(1):
            with self.assertRaises(ConnectionError):
                await self.client.next_message()
            with self.assertRaises(ConnectionError):
                await self.client.get("key")
        self.writer.close.assert_called_once()

    async def test_close_before_pump_starts(self) -> None:
        client = Client(asyncio.StreamReader(), self.writer, CodecChain())
        await client.close()
        async with asyncio.timeout(1):
            with self.assertRaises(ConnectionError):
                await client.next_message()
