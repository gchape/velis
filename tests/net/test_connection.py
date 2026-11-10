import asyncio
import struct
import unittest
from unittest.mock import Mock, patch

from tests.net.helpers import RecordingTransport
from velis.app import build_engine
from velis.core.messages import Err, Message, Nil, Ok, Read, Value, Write
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.connection import COMMANDS_PER_TURN, REPLY_BYTES_PER_TURN, Connection


class ConnectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.engine = build_engine()
        self.transport = RecordingTransport()
        self.release = Mock()
        self.connection = Connection(
            self.engine, 16, admit=lambda: True, release=self.release
        )
        self.connection.connection_made(self.transport)
        self.addCleanup(self.connection.connection_lost, None)

    def test_fragmented_requests_execute_only_after_complete_frame(self) -> None:
        frame = protocol.encode_command(Write("k", b"v"))
        for byte in frame[:-1]:
            self.connection.data_received(bytes([byte]))
            self.assertEqual(self.transport.writes, [])
        self.connection.data_received(frame[-1:])
        self.assertEqual(self.transport.messages(), [Ok()])

    async def test_command_budget_yields_and_preserves_reply_order(self) -> None:
        total = COMMANDS_PER_TURN + 3
        commands = [Write(str(i), str(i).encode()) for i in range(total)]
        self.connection.data_received(b"".join(map(protocol.encode_command, commands)))
        self.assertEqual(len(self.transport.messages()), COMMANDS_PER_TURN)
        self.assertFalse(self.transport.reading)
        await asyncio.sleep(0)
        self.assertEqual(self.transport.messages(), [Ok()] * total)
        self.assertTrue(self.transport.reading)
        self.connection.data_received(protocol.encode_command(Read(str(total - 1))))
        self.assertEqual(self.transport.messages()[-1], Value(str(total - 1).encode()))

    async def test_reply_byte_budget_yields_before_command_budget(self) -> None:
        value = b"x" * REPLY_BYTES_PER_TURN
        self.connection.data_received(protocol.encode_command(Write("large", value)))
        self.transport.writes.clear()
        self.connection.data_received(protocol.encode_command(Read("large")) * 2)
        self.assertEqual(self.transport.messages(), [Value(value)])
        self.assertFalse(self.transport.reading)
        await asyncio.sleep(0)
        self.assertEqual(self.transport.messages(), [Value(value), Value(value)])

    async def test_write_backpressure_buffers_commands_until_resume(self) -> None:
        self.connection.pause_writing()
        self.assertFalse(self.transport.reading)
        self.connection.data_received(protocol.encode_command(Write("k", b"v")))
        self.assertEqual(self.transport.writes, [])
        self.connection.resume_writing()
        self.assertEqual(self.transport.messages(), [Ok()])
        self.assertTrue(self.transport.reading)

    async def test_eof_while_write_blocked_drains_complete_requests_on_resume(
        self,
    ) -> None:
        self.connection.pause_writing()
        self.connection.data_received(protocol.encode_command(Read("k")) * 2 + b"\x00")
        self.assertTrue(self.connection.eof_received())
        self.assertFalse(self.transport.closing)
        self.connection.resume_writing()
        self.assertEqual(self.transport.messages(), [Nil(), Nil()])
        self.assertTrue(self.transport.closing)
        self.assertFalse(self.transport.reading)

    async def test_eof_drains_multiple_turns_and_discards_partial_tail(self) -> None:
        total = COMMANDS_PER_TURN * 2 + 1
        self.connection.data_received(
            protocol.encode_command(Read("k")) * total + b"\x00"
        )
        self.connection.eof_received()
        for _ in range(3):
            await asyncio.sleep(0)
        self.assertEqual(self.transport.messages(), [Nil()] * total)
        self.assertTrue(self.transport.closing)

    def test_protocol_error_flushes_prior_replies_and_discards_remaining_commands(
        self,
    ) -> None:
        good = protocol.encode_command(Write("before", b"v"))
        bad = struct.pack(">I", protocol.MAX_COMMAND_BODY_BYTES + 1)
        after = protocol.encode_command(Write("after", b"v"))
        with self.assertLogs("velis.net.connection", "WARNING"):
            self.connection.data_received(good + bad + after)
        replies = self.transport.messages()
        self.assertEqual(replies[0], Ok())
        self.assertIsInstance(replies[1], Err)
        self.assertTrue(self.transport.closing)
        self.assertFalse(self.transport.aborted)
        self.connection.data_received(good)
        self.assertEqual(self.transport.messages(), replies)

    def test_admission_rejection_does_not_release_an_unowned_slot(self) -> None:
        rejected = Connection(
            self.engine, 16, admit=lambda: False, release=self.release
        )
        transport = RecordingTransport()
        rejected.connection_made(transport)
        self.assertEqual(transport.messages(), [Err("connection limit exceeded")])
        self.assertTrue(transport.closing)
        rejected.connection_lost(None)
        self.release.assert_not_called()

    async def test_disconnect_releases_once_and_cancels_scheduled_work(self) -> None:
        self.connection.data_received(
            protocol.encode_command(Read("k")) * (COMMANDS_PER_TURN + 1)
        )
        with patch.object(self.engine, "disconnect") as disconnect:
            self.connection.connection_lost(None)
            self.connection.connection_lost(None)
            disconnect.assert_called_once_with(self.connection)
        await asyncio.sleep(0)
        self.release.assert_called_once_with()
        self.assertEqual(len(self.transport.messages()), COMMANDS_PER_TURN)
        self.assertFalse(self.connection.send(Message("a", b"v")))

    def test_os_write_failure_aborts_without_unexpected_error_logging(self) -> None:
        self.transport.write_error = OSError("write failed")
        with self.assertNoLogs("velis.net.connection", "ERROR"):
            self.connection.data_received(protocol.encode_command(Read("k")))
        self.assertTrue(self.transport.aborted)

    def test_unexpected_execution_failure_is_logged_and_aborted(self) -> None:
        with (
            patch.object(
                self.engine, "execute", side_effect=RuntimeError("engine failed")
            ),
            self.assertLogs("velis.net.connection", "ERROR") as logs,
        ):
            self.connection.data_received(protocol.encode_command(Read("k")))
        self.assertIn("engine failed", "\n".join(logs.output))
        self.assertTrue(self.transport.aborted)

    def test_push_buffer_limit_is_strictly_greater_than(self) -> None:
        message = Message("a", b"v")
        self.transport.buffer_size = 16
        self.assertTrue(self.connection.send(message))
        self.assertEqual(self.transport.messages(), [message])
        self.transport.buffer_size = 17
        self.assertFalse(self.connection.send(message))
        self.assertTrue(self.transport.aborted)

    def test_expected_push_failures_abort_and_return_false(self) -> None:
        for error in (ProtocolError("bad push"), OSError("broken pipe")):
            with self.subTest(error=error):
                self.transport.closing = self.transport.aborted = False
                with patch("velis.net.protocol.encode_push", side_effect=error):
                    if isinstance(error, ProtocolError):
                        with self.assertLogs("velis.net.connection", "WARNING"):
                            self.assertFalse(self.connection.send(Message("a", b"v")))
                    else:
                        self.assertFalse(self.connection.send(Message("a", b"v")))
                self.assertTrue(self.transport.aborted)

    def test_unexpected_push_failure_propagates(self) -> None:
        self.transport.write_error = RuntimeError("transport failed")
        with self.assertRaisesRegex(RuntimeError, "transport failed"):
            self.connection.send(Message("a", b"v"))
        self.assertFalse(self.transport.aborted)
