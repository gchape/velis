import asyncio
import unittest
from unittest.mock import Mock

from tests.codec.helpers import Reverse
from tests.helpers import RecordingSubscriber
from velis.codec import CodecChain, Deflate
from velis.core.broker import Broker
from velis.core.commands import Message
from velis.net import protocol
from velis.net.connection import Connection, share_message_encodings


class ConnectionDeliveryTests(unittest.TestCase):
    def test_publish_reuses_encoding_per_wire_and_releases_cache_after_dispatch(
        self,
    ) -> None:
        for transformed in (False, True):
            with self.subTest(transformed=transformed):
                codec = Mock(wraps=protocol.FrameCodec())
                wire = protocol.WireProtocol(frame_codec=codec)
                if transformed:
                    wire = protocol.WireProtocol(
                        frame_codec=CodecChain.of(codec) | Reverse()
                    )
                other_codec = Mock(wraps=protocol.FrameCodec())
                other_wire = protocol.WireProtocol(frame_codec=other_codec)
                first = Connection(Mock(), self.writer, 16, wire=wire)
                second = Connection(Mock(), self.writer, 16, wire=wire)
                third = Connection(Mock(), self.writer, 16, wire=other_wire)
                for expected_calls in (1, 2):
                    self.writer.write.reset_mock()
                    with share_message_encodings():
                        self.assertTrue(first.send("news", b"hello"))
                        self.assertTrue(second.send("news", b"hello"))
                        self.assertTrue(third.send("news", b"hello"))
                    self.assertEqual(codec.encode.call_count, expected_calls)
                    self.assertEqual(other_codec.encode.call_count, expected_calls)
                    first_data, second_data, third_data = (
                        call.args[0] for call in self.writer.write.call_args_list
                    )
                    self.assertIs(first_data, second_data)
                    self.assertEqual(
                        wire.frames.decode(first_data), Message("news", b"hello")
                    )
                    self.assertEqual(
                        other_wire.frames.decode(third_data), Message("news", b"hello")
                    )

    def setUp(self) -> None:
        self.writer = Mock(spec=asyncio.StreamWriter)
        self.writer.is_closing.return_value = False
        self.writer.transport = Mock(spec=asyncio.Transport)
        self.writer.transport.get_write_buffer_size.return_value = 0
        self.connection = Connection(Mock(spec=asyncio.StreamReader), self.writer, 16)

    def test_write_failure_drops_subscriber_and_preserves_fanout(self) -> None:
        """Expected socket failures cannot interrupt fanout to healthy peers."""
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
        self.writer.is_closing.return_value = True
        self.assertFalse(self.connection.send("news", b"hello"))
        self.writer.write.assert_not_called()

    def test_buffer_limit_accepts_boundary_and_rejects_overflow(self) -> None:
        self.writer.transport.get_write_buffer_size.return_value = 16
        self.assertTrue(self.connection.send("news", b"hello"))
        self.writer.transport.abort.assert_not_called()
        self.writer.transport.get_write_buffer_size.return_value = 17
        self.assertFalse(self.connection.send("news", b"hello"))
        self.writer.transport.abort.assert_called_once()

    def test_encoding_failure_drops_only_subscriber_and_preserves_fanout(self) -> None:
        wire = protocol.WireProtocol(
            frame_codec=CodecChain.of(protocol.FrameCodec()) | Deflate(max_length=64)
        )
        failing = Connection(Mock(), self.writer, 16, wire=wire)
        broker, healthy = Broker(), RecordingSubscriber()
        for channel in ("news", "other"):
            broker.subscribe(channel, failing)
            broker.subscribe(channel, healthy)
        for cached in (False, True):
            with self.subTest(cached=cached):
                broker.subscribe("news", failing)
                if cached:
                    with share_message_encodings():
                        accepted = broker.publish("news", b"x" * 128)
                else:
                    accepted = broker.publish("news", b"x" * 128)
                self.assertEqual(accepted, 1)
                self.assertEqual(broker.subscribers("news"), 1)
                self.assertEqual(broker.subscribers("other"), 1)
        self.assertEqual(healthy.messages, [("news", b"x" * 128)] * 2)
        self.writer.write.assert_not_called()
        self.assertEqual(self.writer.transport.abort.call_count, 2)
