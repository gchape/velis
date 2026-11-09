import asyncio
import unittest
from unittest.mock import Mock

from tests.net.helpers import make_connection
from velis.core.engine import Engine
from velis.core.messages import Get, Ok
from velis.net import protocol
from velis.net.connection import COMMANDS_PER_TURN


class SchedulingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.engine = Mock(spec=Engine)
        self.engine.execute.return_value = Ok()
        self.transport = Mock(spec=asyncio.Transport)
        self.transport.is_closing.return_value = False
        self.connection = make_connection(self.transport, engine=self.engine)
        self.addCleanup(self.connection.connection_lost, None)
        self.frame = protocol.encode_command(Get("key"))

    async def test_unexpected_command_failure_aborts_the_connection(self) -> None:
        self.engine.execute.side_effect = RuntimeError("command failed")
        with self.assertLogs("velis.net.connection", level="ERROR"):
            self.connection.data_received(self.frame)
        self.transport.abort.assert_called_once()
        self.transport.write.assert_not_called()

    async def test_ready_pipeline_yields_between_command_batches(self) -> None:
        total = COMMANDS_PER_TURN * 3 + 1
        self.connection.data_received(self.frame * (total - 1))
        self.connection.data_received(self.frame)
        self.assertEqual(self.engine.execute.call_count, COMMANDS_PER_TURN)
        progress = [self.engine.execute.call_count]
        async with asyncio.timeout(1):
            while self.engine.execute.call_count < total:
                await asyncio.sleep(0)
                progress.append(self.engine.execute.call_count)
        self.assertEqual(
            progress,
            [COMMANDS_PER_TURN, COMMANDS_PER_TURN * 2, COMMANDS_PER_TURN * 3, total],
        )
        self.assertEqual(self.transport.write.call_count, 4)
        self.transport.pause_reading.assert_called_once()
        self.transport.resume_reading.assert_called_once()

    async def test_connection_loss_cancels_scheduled_commands(self) -> None:
        self.connection.data_received(self.frame * (COMMANDS_PER_TURN + 1))
        self.connection.connection_lost(None)
        await asyncio.sleep(0)
        self.assertEqual(self.engine.execute.call_count, COMMANDS_PER_TURN)

    async def test_write_backpressure_defers_buffered_commands_until_resume(
        self,
    ) -> None:
        self.connection.pause_writing()
        self.connection.data_received(self.frame * 2)
        self.engine.execute.assert_not_called()
        self.transport.pause_reading.assert_called_once()
        self.connection.resume_writing()
        self.assertEqual(self.engine.execute.call_count, 2)
        self.transport.resume_reading.assert_called_once()

    async def test_backpressure_during_a_batch_keeps_reading_paused(self) -> None:
        self.transport.write.side_effect = lambda _: self.connection.pause_writing()
        self.connection.data_received(self.frame * (COMMANDS_PER_TURN + 1))
        await asyncio.sleep(0)
        self.assertEqual(self.engine.execute.call_count, COMMANDS_PER_TURN)
        self.transport.resume_reading.assert_not_called()
        self.transport.write.side_effect = None
        self.connection.resume_writing()
        self.assertEqual(self.engine.execute.call_count, COMMANDS_PER_TURN + 1)
        self.transport.resume_reading.assert_called_once()

    async def test_failed_batch_write_aborts_without_leaking_an_oserror(self) -> None:
        self.transport.write.side_effect = OSError("disconnected")
        self.connection.data_received(self.frame)
        self.transport.abort.assert_called_once()

    async def test_unexpected_batch_write_failure_logs_and_aborts(self) -> None:
        self.transport.write.side_effect = RuntimeError("write failed")
        with self.assertLogs("velis.net.connection", level="ERROR"):
            self.connection.data_received(self.frame)
        self.transport.abort.assert_called_once()

    async def test_eof_drains_queued_batches_and_discards_partial_tail(self) -> None:
        closed = asyncio.Event()
        self.transport.close.side_effect = closed.set
        total = COMMANDS_PER_TURN * 2 + 1
        self.connection.data_received(self.frame * total + self.frame[:-1])
        self.assertTrue(self.connection.eof_received())
        async with asyncio.timeout(1):
            await closed.wait()
        self.assertEqual(self.engine.execute.call_count, total)
        encoded = b"".join(call.args[0] for call in self.transport.write.call_args_list)
        replies = [
            protocol.SERVER_MESSAGES.decode(body)
            for body, _ in protocol.iter_frames(bytearray(encoded))
        ]
        self.assertEqual(replies, [Ok()] * total)
