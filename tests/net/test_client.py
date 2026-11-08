import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from velis.core.commands import Message, Ok, Subscribed, Unsubscribed, Value
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.client import Client
from velis.net.session import TcpSession


class ClientLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.reader = asyncio.StreamReader()
        self.writer = Mock(spec=asyncio.StreamWriter)
        self.writer.drain = AsyncMock()
        self.writer.wait_closed = AsyncMock()
        self.client = Client(
            TcpSession(
                self.reader,
                self.writer,
                max_pending_messages=2,
                max_pending_bytes=8,
            )
        )
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

    async def test_close_wakes_all_waiting_message_readers(self) -> None:
        pending = [asyncio.create_task(self.client.next_message()) for _ in range(3)]
        await asyncio.sleep(0)
        await self.client.close()
        async with asyncio.timeout(1):
            results = await asyncio.gather(*pending, return_exceptions=True)
        self.assertTrue(all(isinstance(result, ConnectionError) for result in results))

    async def test_local_close_preserves_a_full_message_queue(self) -> None:
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(Message("", b"a"))
            + protocol.DEFAULT_PROTOCOL.frames.encode(Message("", b"b"))
        )
        await asyncio.sleep(0)
        await self.client.close()
        await self.client.close()
        async with asyncio.timeout(1):
            self.assertEqual(await self.client.next_message(), Message("", b"a"))
            self.assertEqual(await self.client.next_message(), Message("", b"b"))
            with self.assertRaises(ConnectionError):
                await self.client.next_message()
        self.writer.close.assert_called_once()

    async def test_cancelled_message_wait_leaves_connection_usable(self) -> None:
        pending = asyncio.create_task(self.client.next_message())
        await asyncio.sleep(0)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(Message("ch", b"next"))
        )
        async with asyncio.timeout(1):
            self.assertEqual(await self.client.next_message(), Message("ch", b"next"))
        self.writer.close.assert_not_called()

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
        client = Client(TcpSession(asyncio.StreamReader(), self.writer))
        await client.close()
        async with asyncio.timeout(1):
            with self.assertRaises(ConnectionError):
                await client.next_message()

    async def test_concurrent_requests_remain_serialized(self) -> None:
        first = asyncio.create_task(self.client.get("first"))
        second = asyncio.create_task(self.client.get("second"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.assertEqual(self.writer.write.call_count, 1)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Value(b"one"))
            )
            self.assertEqual(await first, b"one")
            await asyncio.sleep(0)
            self.assertEqual(self.writer.write.call_count, 2)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Value(b"two"))
            )
            self.assertEqual(await second, b"two")

    async def test_invalid_outgoing_data_leaves_connection_usable(self) -> None:
        with self.assertRaises(ProtocolError):
            await self.client.set("key", b"x" * (protocol.MAX_LEN + 1))
        self.writer.write.assert_not_called()
        pending = asyncio.create_task(self.client.get("key"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.reader.feed_data(protocol.DEFAULT_PROTOCOL.frames.encode(Value(b"ok")))
            self.assertEqual(await pending, b"ok")

    async def test_message_count_overflow_preserves_queued_messages(self) -> None:
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(Message("", b"")) * 3
        )
        async with asyncio.timeout(1):
            for _ in range(2):
                self.assertEqual(await self.client.next_message(), Message("", b""))
            with self.assertRaisesRegex(ConnectionError, "buffer limit exceeded"):
                await self.client.next_message()
        self.writer.close.assert_called_once()

    async def test_message_byte_overflow_wakes_pending_request(self) -> None:
        pending = asyncio.create_task(self.client.get("key"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Message("news", b"12345"))
            )
            with self.assertRaisesRegex(ConnectionError, "buffer limit exceeded"):
                await pending
            with self.assertRaises(ConnectionError):
                await self.client.next_message()

    async def test_reading_messages_releases_byte_budget(self) -> None:
        frame = protocol.DEFAULT_PROTOCOL.frames.encode(Message("ch", b"123456"))
        async with asyncio.timeout(1):
            for _ in range(3):
                self.reader.feed_data(frame)
                self.assertEqual(
                    await self.client.next_message(), Message("ch", b"123456")
                )
        self.writer.close.assert_not_called()

    async def test_full_message_buffer_does_not_block_replies(self) -> None:
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(Message("", b"x")) * 2
        )
        pending = asyncio.create_task(self.client.get("key"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Value(b"reply"))
            )
            self.assertEqual(await pending, b"reply")
            for _ in range(2):
                self.assertEqual(await self.client.next_message(), Message("", b"x"))

    async def test_interleaved_publications_and_reply_keep_their_order(self) -> None:
        request = asyncio.create_task(self.client.get("key"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Message("", b"one"))
                + protocol.DEFAULT_PROTOCOL.frames.encode(Value(b"reply"))
                + protocol.DEFAULT_PROTOCOL.frames.encode(Message("", b"two"))
            )
            self.assertEqual(await request, b"reply")
            self.assertEqual(await self.client.next_message(), Message("", b"one"))
            self.assertEqual(await self.client.next_message(), Message("", b"two"))

    async def test_acknowledgments_validate_type_and_channel(self) -> None:
        cases = (
            (lambda: self.client.set("key", b"value"), Value(b"unexpected")),
            (lambda: self.client.subscribe("news"), Subscribed("other")),
            (lambda: self.client.unsubscribe("news"), Unsubscribed("other")),
        )
        for operation, reply in cases:
            with self.subTest(reply=reply):
                async with asyncio.timeout(1):
                    request = asyncio.create_task(operation())
                    await asyncio.sleep(0)
                    self.reader.feed_data(
                        protocol.DEFAULT_PROTOCOL.frames.encode(reply)
                    )
                    with self.assertRaises(ProtocolError):
                        await request

    async def test_unsolicited_reply_closes_connection(self) -> None:
        self.reader.feed_data(protocol.DEFAULT_PROTOCOL.frames.encode(Ok()))
        async with asyncio.timeout(1):
            with self.assertRaisesRegex(ConnectionError, "without a pending request"):
                await self.client.next_message()
        self.writer.close.assert_called_once()

    async def test_drain_failure_closes_connection(self) -> None:
        self.writer.drain.side_effect = ConnectionError("write failed")
        with self.assertRaisesRegex(ConnectionError, "write failed"):
            await self.client.get("key")
        self.writer.close.assert_called_once()
