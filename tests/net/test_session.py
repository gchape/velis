import asyncio
import unittest
from collections.abc import Iterable
from unittest.mock import AsyncMock, Mock

from tests.net.helpers import cancel_task
from velis.config import DEFAULT_SESSION_LIMITS, SessionLimits
from velis.core.messages import (
    Command,
    Err,
    Message,
    Nil,
    Ok,
    Read,
    Reply,
    Value,
    Write,
)
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.session import TCPSession


class SessionTests(unittest.IsolatedAsyncioTestCase):
    def make_session(
        self, limits: SessionLimits = DEFAULT_SESSION_LIMITS
    ) -> tuple[TCPSession, asyncio.StreamReader, Mock]:
        reader = asyncio.StreamReader()
        writer = Mock(spec=asyncio.StreamWriter)
        writer.drain = AsyncMock()
        writer.wait_closed = AsyncMock()
        session = TCPSession(reader, writer, limits=limits)
        self.addAsyncCleanup(session.close)
        return session, reader, writer

    async def start_request(self, session: TCPSession, key: str) -> asyncio.Task:
        task = asyncio.create_task(session.request(Read(key)))
        self.addAsyncCleanup(cancel_task, task)
        await asyncio.sleep(0)
        return task

    async def start_batch(
        self, session: TCPSession, commands: Iterable[Command]
    ) -> asyncio.Task[list[Reply]]:
        task = asyncio.create_task(session.batch(commands))
        self.addAsyncCleanup(cancel_task, task)
        await asyncio.sleep(0)
        return task

    async def test_batch_writes_commands_together_and_separates_pushes(self) -> None:
        session, reader, writer = self.make_session()
        commands = [Write("k", b"v"), Read("k"), Read("missing")]
        batch = await self.start_batch(session, commands)
        writer.write.assert_called_once_with(
            b"".join(map(protocol.encode_command, commands))
        )
        writer.drain.assert_awaited_once_with()
        reader.feed_data(
            b"".join(
                map(
                    protocol.encode_message,
                    (Ok(), Message("c", b"push"), Err("rejected"), Nil()),
                )
            )
        )
        async with asyncio.timeout(1):
            self.assertEqual(await batch, [Ok(), Err("rejected"), Nil()])
            self.assertEqual(await session.next_message(), Message("c", b"push"))

    async def test_empty_batch_does_no_io(self) -> None:
        session, _, writer = self.make_session()
        self.assertEqual(await session.batch([]), [])
        writer.write.assert_not_called()
        writer.drain.assert_not_awaited()

    async def test_batch_validation_rejects_all_commands_before_waiting_for_a_slot(
        self,
    ) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=1)
        )
        pending = await self.start_request(session, "pending")
        async with asyncio.timeout(1):
            with self.assertRaises(ProtocolError):
                await session.batch([Write("new", b"v"), Read("")])
        writer.write.assert_called_once_with(protocol.encode_command(Read("pending")))
        writer.close.assert_not_called()
        reader.feed_data(protocol.encode_message(Nil()))
        self.assertEqual(await pending, Nil())

    async def test_batch_accepts_a_generator_and_splits_at_the_request_limit(
        self,
    ) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=2)
        )
        commands = [Read(str(i)) for i in range(5)]
        batch = await self.start_batch(session, iter(commands))
        writer.write.assert_called_once_with(
            b"".join(map(protocol.encode_command, commands[:2]))
        )
        reader.feed_data(protocol.encode_message(Value(b"0")))
        await asyncio.sleep(0)
        self.assertEqual(writer.write.call_count, 1)
        reader.feed_data(protocol.encode_message(Value(b"1")))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(writer.write.call_count, 2)
        self.assertEqual(
            writer.write.call_args.args[0],
            b"".join(map(protocol.encode_command, commands[2:4])),
        )
        reader.feed_data(
            protocol.encode_message(Value(b"2")) + protocol.encode_message(Value(b"3"))
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(writer.write.call_count, 3)
        self.assertEqual(
            writer.write.call_args.args[0], protocol.encode_command(commands[4])
        )
        reader.feed_data(protocol.encode_message(Value(b"4")))
        async with asyncio.timeout(1):
            self.assertEqual(await batch, [Value(str(i).encode()) for i in range(5)])
        self.assertEqual(writer.drain.await_count, 3)

    async def test_batch_byte_target_includes_framing_and_accepts_exact_fit(
        self,
    ) -> None:
        commands = [Read("😀"), Read("é"), Read("last")]
        frames = list(map(protocol.encode_command, commands))
        session, reader, writer = self.make_session(
            SessionLimits(max_batch_bytes=len(frames[0]) + len(frames[1]))
        )
        batch = await self.start_batch(session, commands)
        writer.write.assert_called_once_with(frames[0] + frames[1])
        reader.feed_data(protocol.encode_message(Nil()) * 2)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(writer.write.call_args.args[0], frames[2])
        reader.feed_data(protocol.encode_message(Nil()))
        async with asyncio.timeout(1):
            self.assertEqual(await batch, [Nil()] * 3)

    async def test_command_larger_than_batch_target_is_sent_alone(self) -> None:
        commands = [Write("k", b"x" * 100), Read("k")]
        session, reader, writer = self.make_session(SessionLimits(max_batch_bytes=1))
        batch = await self.start_batch(session, commands)
        writer.write.assert_called_once_with(protocol.encode_command(commands[0]))
        reader.feed_data(protocol.encode_message(Ok()))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(writer.write.call_count, 2)
        self.assertEqual(
            writer.write.call_args.args[0], protocol.encode_command(commands[1])
        )
        reader.feed_data(protocol.encode_message(Value(b"x" * 100)))
        async with asyncio.timeout(1):
            self.assertEqual(await batch, [Ok(), Value(b"x" * 100)])

    async def test_batch_uses_only_slots_not_occupied_by_other_requests(self) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=3)
        )
        pending = await self.start_request(session, "pending")
        commands = [Read(str(i)) for i in range(3)]
        batch = await self.start_batch(session, commands)
        self.assertEqual(writer.write.call_count, 2)
        self.assertEqual(
            writer.write.call_args.args[0],
            b"".join(map(protocol.encode_command, commands[:2])),
        )
        reader.feed_data(
            b"".join(
                map(
                    protocol.encode_message,
                    (Value(b"pending"), Value(b"0"), Value(b"1")),
                )
            )
        )
        async with asyncio.timeout(1):
            self.assertEqual(await pending, Value(b"pending"))
            await asyncio.sleep(0)
            self.assertEqual(writer.write.call_count, 3)
            reader.feed_data(protocol.encode_message(Value(b"2")))
            self.assertEqual(await batch, [Value(b"0"), Value(b"1"), Value(b"2")])

    async def test_concurrent_batches_and_requests_match_their_own_replies(
        self,
    ) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=3)
        )
        windows: list[int] = []

        def echo(data: bytes) -> None:
            commands = [
                protocol.COMMANDS.decode(body)
                for body, _ in protocol.iter_frames(bytearray(data))
            ]
            windows.append(len(commands))
            for command in commands:
                assert isinstance(command, Read)
                reader.feed_data(protocol.encode_message(Value(command.key.encode())))

        async def drain() -> None:
            await asyncio.sleep(0)

        writer.write.side_effect = echo
        writer.drain.side_effect = drain
        first = [Read(f"a{i}") for i in range(10)]
        second = [Read(f"b{i}") for i in range(7)]
        async with asyncio.timeout(1):
            replies = await asyncio.gather(
                session.batch(first),
                session.batch(second),
                session.request(Read("single")),
            )
        self.assertEqual(
            replies,
            [
                [Value(command.key.encode()) for command in first],
                [Value(command.key.encode()) for command in second],
                Value(b"single"),
            ],
        )
        self.assertTrue(all(1 <= size <= 3 for size in windows))
        self.assertIn(3, windows)

    async def test_concurrent_windows_share_backpressure_without_serializing_writes(
        self,
    ) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=4)
        )
        resume = asyncio.Event()
        writer.drain.side_effect = resume.wait
        first = await self.start_batch(session, [Read("a"), Read("b")])
        second = await self.start_batch(session, [Read("c"), Read("d")])
        self.assertEqual(writer.write.call_count, 2)
        blocked = await self.start_request(session, "e")
        self.assertEqual(writer.write.call_count, 2)
        await cancel_task(first)
        reader.feed_data(
            b"".join(protocol.encode_message(Value(key.encode())) for key in "abcd")
        )
        resume.set()
        async with asyncio.timeout(1):
            self.assertEqual(await second, [Value(b"c"), Value(b"d")])
            await asyncio.sleep(0)
            reader.feed_data(protocol.encode_message(Value(b"e")))
            self.assertEqual(await blocked, Value(b"e"))
        writer.close.assert_not_called()

    async def test_cancelled_batch_retains_sent_slots_and_sends_no_more_windows(
        self,
    ) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=2)
        )
        batch = await self.start_batch(session, [Read("a"), Read("b"), Read("unsent")])
        await cancel_task(batch)
        following = await self.start_request(session, "following")
        self.assertEqual(writer.write.call_count, 1)
        reader.feed_data(protocol.encode_message(Value(b"late-a")))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(writer.write.call_count, 2)
        reader.feed_data(
            protocol.encode_message(Value(b"late-b"))
            + protocol.encode_message(Value(b"following"))
        )
        async with asyncio.timeout(1):
            self.assertEqual(await following, Value(b"following"))
        self.assertEqual(
            writer.write.call_args.args[0], protocol.encode_command(Read("following"))
        )
        writer.close.assert_not_called()

    async def test_cancelling_batch_waiting_for_capacity_sends_nothing(self) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=1)
        )
        pending = await self.start_request(session, "pending")
        blocked = await self.start_batch(
            session, [Read("blocked-a"), Read("blocked-b")]
        )
        await cancel_task(blocked)
        reader.feed_data(protocol.encode_message(Nil()))
        self.assertEqual(await pending, Nil())
        writer.write.assert_called_once_with(protocol.encode_command(Read("pending")))
        following = await self.start_request(session, "following")
        reader.feed_data(protocol.encode_message(Value(b"following")))
        self.assertEqual(await following, Value(b"following"))

    async def test_cancelling_batch_during_drain_preserves_late_reply_alignment(
        self,
    ) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=2)
        )
        draining = asyncio.Event()

        async def drain() -> None:
            draining.set()
            await asyncio.Future()

        writer.drain.side_effect = drain
        batch = await self.start_batch(session, [Read("a"), Read("b")])
        self.assertTrue(draining.is_set())
        await cancel_task(batch)
        writer.drain.side_effect = None
        following = await self.start_request(session, "following")
        self.assertEqual(writer.write.call_count, 1)
        reader.feed_data(protocol.encode_message(Nil()) * 2)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        reader.feed_data(protocol.encode_message(Value(b"following")))
        async with asyncio.timeout(1):
            self.assertEqual(await following, Value(b"following"))
        writer.close.assert_not_called()

    async def test_close_wakes_an_active_batch_and_a_batch_waiting_for_capacity(
        self,
    ) -> None:
        session, _, writer = self.make_session(SessionLimits(max_pending_requests=2))
        active = await self.start_batch(session, [Read("a"), Read("b"), Read("unsent")])
        blocked = await self.start_batch(session, [Read("blocked")])
        await session.close()
        async with asyncio.timeout(1):
            for task in (active, blocked):
                with self.assertRaisesRegex(ConnectionError, "connection closed"):
                    await task
        self.assertEqual(writer.write.call_count, 1)

    async def test_batch_write_or_drain_failure_closes_the_session(self) -> None:
        for method in ("write", "drain"):
            with self.subTest(method=method):
                session, _, writer = self.make_session()
                getattr(writer, method).side_effect = OSError("send failed")
                with self.assertRaisesRegex(OSError, "send failed"):
                    await session.batch([Read("a"), Read("b")])
                writer.close.assert_called_once_with()
                with self.assertRaises(ConnectionError):
                    await session.next_message()

    async def test_protocol_failure_during_batch_fails_remaining_requests(self) -> None:
        session, reader, _ = self.make_session()
        batch = await self.start_batch(session, [Read("a"), Read("b")])
        reader.feed_data(
            protocol.encode_message(Value(b"a"))
            + protocol.encode_frame(b"bad", protocol.MAX_SERVER_BODY_BYTES)
        )
        async with asyncio.timeout(1):
            with self.assertRaisesRegex(ConnectionError, "truncated"):
                await batch

    async def test_interleaved_pushes_and_replies_preserve_request_order(self) -> None:
        session, reader, writer = self.make_session()
        first = await self.start_request(session, "a")
        second = await self.start_request(session, "b")
        self.assertEqual(
            [call.args[0] for call in writer.write.call_args_list],
            [protocol.encode_command(Read("a")), protocol.encode_command(Read("b"))],
        )
        reader.feed_data(
            b"".join(
                protocol.encode_message(message)
                for message in (Message("c", b"push"), Value(b"a"), Nil())
            )
        )
        async with asyncio.timeout(1):
            self.assertEqual(await first, Value(b"a"))
            self.assertEqual(await second, Nil())
            self.assertEqual(await session.next_message(), Message("c", b"push"))

    async def test_cancelled_request_keeps_its_slot_until_late_reply(self) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=1)
        )
        first = await self.start_request(session, "first")
        await cancel_task(first)
        second = await self.start_request(session, "second")
        self.assertEqual(writer.write.call_count, 1)
        reader.feed_data(protocol.encode_message(Value(b"late")))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(writer.write.call_count, 2)
        reader.feed_data(protocol.encode_message(Value(b"current")))
        async with asyncio.timeout(1):
            self.assertEqual(await second, Value(b"current"))
        writer.close.assert_not_called()

    async def test_cancelled_slot_waiter_never_writes_a_command(self) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=1)
        )
        first = await self.start_request(session, "first")
        blocked = await self.start_request(session, "blocked")
        await cancel_task(blocked)
        reader.feed_data(protocol.encode_message(Value(b"first")))
        self.assertEqual(await first, Value(b"first"))
        self.assertEqual(writer.write.call_count, 1)
        third = await self.start_request(session, "third")
        reader.feed_data(protocol.encode_message(Value(b"third")))
        self.assertEqual(await third, Value(b"third"))

    async def test_invalid_outgoing_command_uses_no_slot_and_leaves_session_open(
        self,
    ) -> None:
        session, reader, writer = self.make_session(
            SessionLimits(max_pending_requests=1)
        )
        with self.assertRaises(ProtocolError):
            await session.request(Read(""))
        writer.write.assert_not_called()
        writer.close.assert_not_called()
        request = await self.start_request(session, "valid")
        reader.feed_data(protocol.encode_message(Nil()))
        self.assertEqual(await request, Nil())

    async def test_close_wakes_requests_slot_waiters_and_message_waiters(self) -> None:
        session, _, writer = self.make_session(SessionLimits(max_pending_requests=1))
        pending = await self.start_request(session, "pending")
        blocked = await self.start_request(session, "blocked")
        message = asyncio.create_task(session.next_message())
        self.addAsyncCleanup(cancel_task, message)
        await session.close()
        for task in (pending, blocked, message):
            with self.assertRaisesRegex(ConnectionError, "connection closed"):
                await task
        await session.close()
        self.assertEqual(writer.write.call_count, 1)
        writer.close.assert_called_once_with()

    async def test_queued_pushes_drain_after_eof(self) -> None:
        session, reader, _ = self.make_session()
        reader.feed_data(
            protocol.encode_message(Message("a", b"one"))
            + protocol.encode_message(Message("a", b"two"))
        )
        reader.feed_eof()
        self.assertEqual(await session.next_message(), Message("a", b"one"))
        self.assertEqual(await session.next_message(), Message("a", b"two"))
        with self.assertRaisesRegex(ConnectionError, "connection closed"):
            await session.next_message()

    async def test_message_limits_close_and_preserve_queued_push(
        self,
    ) -> None:
        for limits in (
            SessionLimits(max_pending_messages=1),
            SessionLimits(max_pending_bytes=5),
        ):
            with self.subTest(limits=limits):
                session, reader, _ = self.make_session(limits)
                reader.feed_data(
                    protocol.encode_message(Message("😀", b"x"))
                    + protocol.encode_message(Message("a", b"y"))
                )
                await asyncio.sleep(0)
                self.assertEqual(await session.next_message(), Message("😀", b"x"))
                with self.assertRaisesRegex(ConnectionError, "message buffer limit"):
                    await session.next_message()

    async def test_consuming_a_push_frees_its_byte_budget(self) -> None:
        session, reader, _ = self.make_session(SessionLimits(max_pending_bytes=5))
        for _ in range(3):
            reader.feed_data(protocol.encode_message(Message("😀", b"x")))
            self.assertEqual(await session.next_message(), Message("😀", b"x"))

    async def test_cancelled_message_wait_does_not_close_the_session(self) -> None:
        session, reader, writer = self.make_session()
        message = asyncio.create_task(session.next_message())
        await asyncio.sleep(0)
        await cancel_task(message)
        reader.feed_data(protocol.encode_message(Message("a", b"v")))
        self.assertEqual(await session.next_message(), Message("a", b"v"))
        writer.close.assert_not_called()

    async def test_unsolicited_error_preserves_server_rejection_reason(self) -> None:
        session, reader, _ = self.make_session()
        reader.feed_data(protocol.encode_message(Err("connection limit exceeded")))
        with self.assertRaisesRegex(ConnectionError, "connection limit exceeded"):
            await session.next_message()
        with self.assertRaisesRegex(ConnectionError, "connection limit exceeded"):
            await session.request(Read("k"))

    async def test_unsolicited_reply_and_malformed_input_close_with_protocol_reason(
        self,
    ) -> None:
        for frame, reason in (
            (protocol.encode_message(Nil()), "without a pending request"),
            (
                protocol.encode_frame(b"bad", protocol.MAX_SERVER_BODY_BYTES),
                "truncated",
            ),
        ):
            with self.subTest(reason=reason):
                session, reader, _ = self.make_session()
                reader.feed_data(frame)
                with self.assertRaisesRegex(ConnectionError, reason):
                    await session.next_message()

    async def test_incomplete_input_and_os_errors_close_normally(self) -> None:
        for error in (asyncio.IncompleteReadError(b"x", 4), OSError("reset")):
            with self.subTest(error=error):
                session, reader, _ = self.make_session()
                reader.set_exception(error)
                with self.assertRaisesRegex(ConnectionError, "connection closed"):
                    await session.next_message()
                await session.close()

    async def test_unexpected_reader_failure_is_retrieved_and_raised_once_after_cleanup(
        self,
    ) -> None:
        session, reader, writer = self.make_session()
        error = RuntimeError("reader crashed")
        reader.set_exception(error)
        with self.assertRaisesRegex(ConnectionError, "reader failed.*reader crashed"):
            await session.next_message()
        with self.assertRaises(RuntimeError) as caught:
            await session.close()
        self.assertIs(caught.exception, error)
        writer.close.assert_called_once_with()
        writer.wait_closed.assert_awaited_once_with()
        await session.close()

    async def test_request_write_failure_closes_session_and_preserves_error(
        self,
    ) -> None:
        session, _, writer = self.make_session()
        writer.write.side_effect = OSError("write failed")
        with self.assertRaisesRegex(OSError, "write failed"):
            await session.request(Read("k"))
        with self.assertRaises(ConnectionError):
            await session.next_message()
        writer.close.assert_called_once_with()

    async def test_close_suppresses_expected_transport_shutdown_errors(self) -> None:
        session, _, writer = self.make_session()
        writer.wait_closed.side_effect = OSError("already closed")
        await session.close()
        await session.close()
