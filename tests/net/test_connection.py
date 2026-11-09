import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from tests.codec.helpers import Reverse
from tests.helpers import RecordingSubscriber
from velis.codec import CompositeCodec, Identity, LengthPrefixedField, UInt
from velis.core.broker import Broker, Publication
from velis.core.messages import Message
from velis.net import protocol
from velis.net.connection import Connection


class ConnectionDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.writer = Mock(spec=asyncio.StreamWriter)
        self.writer.transport = Mock(spec=asyncio.Transport)
        self.writer.transport.is_closing.return_value = False
        self.writer.transport.get_write_buffer_size.return_value = 0
        self.connection = Connection(Mock(spec=asyncio.StreamReader), self.writer, 16)

    def test_publish_reuses_each_wire_encoding_only_within_one_publication(
        self,
    ) -> None:
        for transformed in (False, True):
            with self.subTest(transformed=transformed):
                codec = Mock(wraps=protocol.SERVER_MESSAGES)
                body = CompositeCodec.of(codec) | Reverse() if transformed else codec
                wire = protocol.WireProtocol(message_codec=body)
                other_codec = Mock(wraps=protocol.SERVER_MESSAGES)
                other_wire = protocol.WireProtocol(message_codec=other_codec)
                peers = (
                    Connection(Mock(), self.writer, 16, wire=wire),
                    Connection(Mock(), self.writer, 16, wire=wire),
                    Connection(Mock(), self.writer, 16, wire=other_wire),
                )
                broker = Broker()
                for peer in peers:
                    broker.subscribe("news", peer)
                for expected_calls in (1, 2):
                    self.writer.write.reset_mock()
                    self.assertEqual(broker.publish("news", b"hello"), 3)
                    self.assertEqual(codec.encode.call_count, expected_calls)
                    self.assertEqual(other_codec.encode.call_count, expected_calls)
                    encoded = [
                        call.args[0] for call in self.writer.write.call_args_list
                    ]
                    self.assertEqual(len(encoded), 3)
                    shared = [
                        data
                        for data in encoded
                        if sum(data is other for other in encoded) == 2
                    ]
                    self.assertEqual(len(shared), 2)
                    self.assertIs(shared[0], shared[1])
                    for data in encoded:
                        decoder = wire if data is shared[0] else other_wire
                        self.assertEqual(
                            decoder.framed_messages.decode(data),
                            Message("news", b"hello"),
                        )

    def test_write_failure_drops_subscriber_and_preserves_fanout(self) -> None:
        self.writer.write.side_effect = ConnectionResetError("peer disconnected")
        broker, healthy = Broker(), RecordingSubscriber()
        for channel in ("news", "other"):
            broker.subscribe(channel, self.connection)
            broker.subscribe(channel, healthy)
        self.assertEqual(broker.publish("news", b"hello"), 1)
        self.assertEqual(healthy.messages, [("news", b"hello")])
        self.assertEqual(broker.subscribers("news"), 1)
        self.assertEqual(broker.subscribers("other"), 1)
        self.writer.transport.abort.assert_called_once()

    def test_closed_transport_rejects_without_writing(self) -> None:
        self.writer.transport.is_closing.return_value = True
        self.assertFalse(self.connection.send(Publication("news", b"hello")))
        self.writer.write.assert_not_called()

    def test_buffer_limit_accepts_boundary_and_rejects_overflow(self) -> None:
        self.writer.transport.get_write_buffer_size.return_value = 16
        self.assertTrue(self.connection.send(Publication("news", b"hello")))
        self.writer.transport.abort.assert_not_called()
        self.writer.transport.get_write_buffer_size.return_value = 17
        self.assertFalse(self.connection.send(Publication("news", b"hello")))
        self.writer.transport.abort.assert_called_once()

    def test_encoding_failure_drops_only_subscriber_and_preserves_fanout(self) -> None:
        wire = protocol.WireProtocol(
            message_codec=CompositeCodec.of(protocol.SERVER_MESSAGES)
            | LengthPrefixedField(Identity(), 64, prefix=UInt(4))
        )
        failing = Connection(Mock(), self.writer, 16, wire=wire)
        broker, healthy = Broker(), RecordingSubscriber()
        for channel in ("news", "other"):
            broker.subscribe(channel, failing)
            broker.subscribe(channel, healthy)
        for _ in range(2):
            broker.subscribe("news", failing)
            self.assertEqual(broker.publish("news", b"x" * 128), 1)
            self.assertEqual(broker.subscribers("news"), 1)
            self.assertEqual(broker.subscribers("other"), 1)
        self.assertEqual(healthy.messages, [("news", b"x" * 128)] * 2)
        self.writer.write.assert_not_called()
        self.assertEqual(self.writer.transport.abort.call_count, 2)


class ConnectionLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_context_closes_stream_and_preserves_caller_failure(self) -> None:
        for failure in (None, RuntimeError("caller failed")):
            with self.subTest(failure=failure):
                writer = Mock(spec=asyncio.StreamWriter)
                writer.wait_closed = AsyncMock()
                connection = Connection(Mock(), writer, 16)
                try:
                    async with connection as active:
                        self.assertIs(active, connection)
                        if failure is not None:
                            raise failure
                except RuntimeError as exc:
                    self.assertIs(exc, failure)
                else:
                    self.assertIsNone(failure)
                writer.close.assert_called_once()
                writer.wait_closed.assert_awaited_once()
