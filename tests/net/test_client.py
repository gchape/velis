import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from velis.core.commands import Err, Get, Message, Ok, Subscribed, Unsubscribed, Value
from velis.errors import CommandError, ProtocolError
from velis.net import protocol
from velis.net.client import Client
from velis.net.protocol import Request, Response
from velis.net.session import TcpSession


class ClientLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.reader = asyncio.StreamReader()
        self.writer = Mock(spec=asyncio.StreamWriter)
        self.writer.drain = AsyncMock()
        self.writer.wait_closed = AsyncMock()
        self.session = TcpSession(
            self.reader,
            self.writer,
            max_pending_requests=2,
            max_pending_messages=2,
            max_pending_bytes=8,
        )
        self.client = Client(self.session)
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

    async def test_cancelled_request_discards_late_reply_and_keeps_connection_usable(
        self,
    ) -> None:
        pending = asyncio.create_task(self.client.get("key"))
        await asyncio.sleep(0)
        self.writer.write.assert_called_once()
        pending.cancel()
        async with asyncio.timeout(1):
            with self.assertRaises(asyncio.CancelledError):
                await pending
            another = asyncio.create_task(self.client.get("another"))
            await asyncio.sleep(0)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"late")))
                + protocol.DEFAULT_PROTOCOL.frames.encode(
                    Response(2, Value(b"current"))
                )
            )
            self.assertEqual(await another, b"current")
        self.writer.close.assert_not_called()

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

    async def test_concurrent_requests_match_out_of_order_replies(self) -> None:
        first = asyncio.create_task(self.client.get("first"))
        second = asyncio.create_task(self.client.get("second"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.assertEqual(self.writer.write.call_count, 2)
            sent = [
                protocol.DEFAULT_PROTOCOL.commands.decode(call.args[0])
                for call in self.writer.write.call_args_list
            ]
            self.assertEqual(
                sent, [Request(1, Get("first")), Request(2, Get("second"))]
            )
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(2, Value(b"two")))
            )
            self.assertEqual(await second, b"two")
            self.assertFalse(first.done())
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"one")))
            )
            self.assertEqual(await first, b"one")

    async def test_invalid_outgoing_data_leaves_connection_usable(self) -> None:
        with self.assertRaises(ProtocolError):
            await self.client.set("key", b"x" * (protocol.MAX_LEN + 1))
        self.writer.write.assert_not_called()
        pending = asyncio.create_task(self.client.get("key"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"ok")))
            )
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
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"reply")))
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
                + protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"reply")))
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
        for request_id, (operation, reply) in enumerate(cases, 1):
            with self.subTest(reply=reply):
                async with asyncio.timeout(1):
                    request = asyncio.create_task(operation())
                    await asyncio.sleep(0)
                    self.reader.feed_data(
                        protocol.DEFAULT_PROTOCOL.frames.encode(
                            Response(request_id, reply)
                        )
                    )
                    with self.assertRaises(ProtocolError):
                        await request

    async def test_unsolicited_reply_closes_connection(self) -> None:
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(Response(42, Ok()))
        )
        async with asyncio.timeout(1):
            with self.assertRaisesRegex(ConnectionError, "unexpected reply ID: 42"):
                await self.client.next_message()
        self.writer.close.assert_called_once()

    async def test_unsolicited_error_preserves_server_rejection_reason(self) -> None:
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(
                Response(0, Err("connection limit exceeded"))
            )
        )
        self.reader.feed_eof()
        async with asyncio.timeout(1):
            with self.assertRaisesRegex(ConnectionError, "connection limit exceeded"):
                await self.client.next_message()
            with self.assertRaisesRegex(ConnectionError, "connection limit exceeded"):
                await self.client.get("key")
        self.writer.close.assert_called_once()

    async def test_command_error_reply_keeps_connection_usable(self) -> None:
        request = asyncio.create_task(self.client.get("key"))
        await asyncio.sleep(0)
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Err("bad command")))
        )
        async with asyncio.timeout(1):
            with self.assertRaisesRegex(CommandError, "bad command"):
                await request
            request = asyncio.create_task(self.client.get("key"))
            await asyncio.sleep(0)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(2, Value(b"ok")))
            )
            self.assertEqual(await request, b"ok")

    async def test_duplicate_reply_closes_connection(self) -> None:
        request = asyncio.create_task(self.client.get("key"))
        await asyncio.sleep(0)
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"ok"))) * 2
        )
        async with asyncio.timeout(1):
            self.assertEqual(await request, b"ok")
            with self.assertRaisesRegex(ConnectionError, "unexpected reply ID: 1"):
                await self.client.next_message()

    async def test_reply_received_during_drain_survives_eof(self) -> None:
        async def drain() -> None:
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"ok")))
            )
            self.reader.feed_eof()
            await asyncio.sleep(0)

        self.writer.drain.side_effect = drain
        async with asyncio.timeout(1):
            self.assertEqual(await self.client.get("key"), b"ok")
            with self.assertRaises(ConnectionError):
                await self.client.get("another")

    async def test_drain_failure_closes_connection(self) -> None:
        self.writer.drain.side_effect = ConnectionError("write failed")
        with self.assertRaisesRegex(ConnectionError, "write failed"):
            await self.client.get("key")
        self.writer.close.assert_called_once()

    async def test_pending_limit_releases_a_slot_for_the_next_request(self) -> None:
        requests = [asyncio.create_task(self.client.get(str(i))) for i in range(3)]
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.assertEqual(self.writer.write.call_count, 2)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(2, Value(b"second")))
            )
            self.assertEqual(await requests[1], b"second")
            await asyncio.sleep(0)
            self.assertEqual(self.writer.write.call_count, 3)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(3, Value(b"third")))
                + protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"first")))
            )
            self.assertEqual(await requests[0], b"first")
            self.assertEqual(await requests[2], b"third")

    async def test_cancelled_requests_retain_bounded_slots_until_replies_arrive(
        self,
    ) -> None:
        requests = [asyncio.create_task(self.client.get(str(i))) for i in range(2)]
        await asyncio.sleep(0)
        for request in requests:
            request.cancel()
        await asyncio.gather(*requests, return_exceptions=True)
        waiting = asyncio.create_task(self.client.get("waiting"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.assertEqual(self.writer.write.call_count, 2)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Err("late error")))
            )
            # The reader releases a slot; the waiting request then sends ID 3.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            self.assertEqual(self.writer.write.call_count, 3)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(3, Value(b"ok")))
                + protocol.DEFAULT_PROTOCOL.frames.encode(Response(2, Value(b"late")))
            )
            self.assertEqual(await waiting, b"ok")
        self.assertEqual(self.session._pending, {})
        self.writer.close.assert_not_called()

    async def test_cancellation_before_a_slot_is_available_does_not_send(self) -> None:
        active = [asyncio.create_task(self.client.get(str(i))) for i in range(2)]
        waiting = asyncio.create_task(self.client.get("cancelled"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            waiting.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiting
            self.assertEqual(self.writer.write.call_count, 2)
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"a")))
                + protocol.DEFAULT_PROTOCOL.frames.encode(Response(2, Value(b"b")))
            )
            self.assertEqual(await asyncio.gather(*active), [b"a", b"b"])
            next_request = asyncio.create_task(self.client.get("next"))
            await asyncio.sleep(0)
            sent = protocol.DEFAULT_PROTOCOL.commands.decode(
                self.writer.write.call_args.args[0]
            )
            self.assertEqual(sent, Request(3, Get("next")))
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(3, Value(b"c")))
            )
            self.assertEqual(await next_request, b"c")

    async def test_cancelling_during_drain_does_not_block_other_requests(self) -> None:
        async def drain() -> None:
            if self.writer.write.call_count == 1:
                await asyncio.Event().wait()

        self.writer.drain.side_effect = drain
        first = asyncio.create_task(self.client.get("first"))
        second = asyncio.create_task(self.client.get("second"))
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            self.assertEqual(self.writer.write.call_count, 2)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            self.reader.feed_data(
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(2, Value(b"second")))
                + protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(b"late")))
            )
            self.assertEqual(await second, b"second")
        self.writer.close.assert_not_called()

    async def test_close_wakes_active_requests_and_all_capacity_waiters(self) -> None:
        requests = [asyncio.create_task(self.client.get(str(i))) for i in range(6)]
        await asyncio.sleep(0)
        self.assertEqual(self.writer.write.call_count, 2)
        await self.client.close()
        async with asyncio.timeout(1):
            results = await asyncio.gather(*requests, return_exceptions=True)
        self.assertTrue(all(isinstance(result, ConnectionError) for result in results))
        self.assertEqual(self.writer.write.call_count, 2)

    async def test_connection_error_fails_every_pending_request_with_its_reason(
        self,
    ) -> None:
        requests = [asyncio.create_task(self.client.get(str(i))) for i in range(3)]
        await asyncio.sleep(0)
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(Response(0, Err("server closing")))
        )
        async with asyncio.timeout(1):
            results = await asyncio.gather(*requests, return_exceptions=True)
        for result in results:
            self.assertIsInstance(result, ConnectionError)
            self.assertEqual(str(result), "server closing")

    async def test_unknown_reply_id_closes_and_fails_pending_requests(self) -> None:
        request = asyncio.create_task(self.client.get("key"))
        await asyncio.sleep(0)
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(Response(77, Value(b"wrong")))
        )
        async with asyncio.timeout(1):
            with self.assertRaisesRegex(ConnectionError, "unexpected reply ID: 77"):
                await request

    async def test_ids_never_wrap_or_reuse_after_exhaustion(self) -> None:
        self.session._next_request_id = protocol.MAX_REQUEST_ID
        request = asyncio.create_task(self.client.get("last"))
        await asyncio.sleep(0)
        self.assertEqual(
            protocol.DEFAULT_PROTOCOL.commands.decode(
                self.writer.write.call_args.args[0]
            ),
            Request(protocol.MAX_REQUEST_ID, Get("last")),
        )
        self.reader.feed_data(
            protocol.DEFAULT_PROTOCOL.frames.encode(
                Response(protocol.MAX_REQUEST_ID, Value(b"last"))
            )
        )
        async with asyncio.timeout(1):
            self.assertEqual(await request, b"last")
            with self.assertRaisesRegex(ConnectionError, "request ID space exhausted"):
                await self.client.get("exhausted")
        self.writer.close.assert_called_once()
        self.writer.write.assert_called_once()

    async def test_invalid_pending_request_limits_do_not_start_the_reader(self) -> None:
        for limit in (0, -1):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                TcpSession(self.reader, self.writer, max_pending_requests=limit)
