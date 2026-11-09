import asyncio
import unittest
from unittest.mock import Mock, patch

from tests.helpers import RecordingSubscriber
from tests.net.helpers import decode_message_frame, make_connection
from velis.core.broker import Broker, Subscriber
from velis.core.messages import Message
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.limits import MAX_FIELD_BYTES


class PushEncoderTests(unittest.TestCase):
    def test_cache_uses_message_identity_and_retains_empty_encodings(self) -> None:
        first, second = Message("news", b"hello"), Message("news", b"hello")
        encode = Mock(side_effect=[b"", b"second"])
        encoder = protocol.PushEncoder(encode)
        for _ in range(2):
            self.assertEqual(encoder(first), b"")
        for _ in range(2):
            self.assertEqual(encoder(second), b"second")
        self.assertEqual(encode.call_count, 2)
        encode.assert_called_with(second)

    def test_new_message_replaces_the_previous_cached_result(self) -> None:
        first, second = Message("news", b"first"), Message("news", b"second")
        encode = Mock(side_effect=[b"first", b"second", b"first again"])
        encoder = protocol.PushEncoder(encode)
        self.assertEqual(encoder(first), b"first")
        self.assertEqual(encoder(second), b"second")
        self.assertEqual(encoder(first), b"first again")
        self.assertEqual(encode.call_count, 3)

    def test_failed_encoding_preserves_the_previous_cached_result(self) -> None:
        first, second = Message("news", b"first"), Message("news", b"second")
        encode = Mock(side_effect=[b"first", ProtocolError("invalid"), b"second"])
        encoder = protocol.PushEncoder(encode)
        self.assertEqual(encoder(first), b"first")
        with self.assertRaises(ProtocolError):
            encoder(second)
        self.assertEqual(encoder(first), b"first")
        self.assertEqual(encoder(second), b"second")
        self.assertEqual(encoder(second), b"second")
        self.assertEqual(encode.call_count, 3)


class ConnectionDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.transport = Mock(spec=asyncio.Transport)
        self.transport.is_closing.return_value = False
        self.transport.get_write_buffer_size.return_value = 0
        self.connection = make_connection(self.transport)

    def test_nested_publication_keeps_each_message_intact(self) -> None:
        broker = Broker()

        def publish_another(message: Message) -> bool:
            if message.payload == b"outer":
                self.assertEqual(broker.publish("news", b"inner"), 2)
            return True

        subscriber = Mock(spec=Subscriber)
        subscriber.send.side_effect = publish_another
        for peer in (self.connection, subscriber):
            broker.subscribe("news", peer)
        self.assertEqual(broker.publish("news", b"outer"), 2)
        messages = [
            decode_message_frame(call.args[0])
            for call in self.transport.write.call_args_list
        ]
        self.assertCountEqual(
            messages, [Message("news", b"outer"), Message("news", b"inner")]
        )

    def test_publish_encodes_once_for_all_subscribers(self) -> None:
        codec = Mock(wraps=protocol.SERVER_MESSAGES)
        peers = tuple(make_connection(self.transport) for _ in range(3))
        broker = Broker()
        for peer in peers:
            broker.subscribe("news", peer)
        with patch.object(protocol, "SERVER_MESSAGES", codec):
            for expected_calls in (1, 2):
                self.transport.write.reset_mock()
                self.assertEqual(broker.publish("news", b"hello"), 3)
                self.assertEqual(codec.encode.call_count, expected_calls)
                encoded = [call.args[0] for call in self.transport.write.call_args_list]
                self.assertEqual(len(encoded), 3)
                self.assertTrue(all(data is encoded[0] for data in encoded))
                self.assertEqual(
                    decode_message_frame(encoded[0]), Message("news", b"hello")
                )

    def test_write_failure_drops_subscriber_and_preserves_fanout(self) -> None:
        self.transport.write.side_effect = ConnectionResetError("peer disconnected")
        broker, healthy = Broker(), RecordingSubscriber()
        for channel in ("news", "other"):
            broker.subscribe(channel, self.connection)
            broker.subscribe(channel, healthy)
        self.assertEqual(broker.publish("news", b"hello"), 1)
        self.assertEqual(healthy.messages, [("news", b"hello")])
        self.assertEqual(broker.subscribers("news"), 1)
        self.assertEqual(broker.subscribers("other"), 1)
        self.transport.abort.assert_called_once()

    def test_closed_transport_rejects_without_writing(self) -> None:
        self.transport.is_closing.return_value = True
        self.assertFalse(self.connection.send(Message("news", b"hello")))
        self.transport.write.assert_not_called()

    def test_buffer_limit_accepts_boundary_and_rejects_overflow(self) -> None:
        self.transport.get_write_buffer_size.return_value = 16
        self.assertTrue(self.connection.send(Message("news", b"hello")))
        self.transport.abort.assert_not_called()
        self.transport.get_write_buffer_size.return_value = 17
        self.assertFalse(self.connection.send(Message("news", b"hello")))
        self.transport.abort.assert_called_once()

    def test_encoding_failure_drops_network_subscriber_and_preserves_fanout(
        self,
    ) -> None:
        broker, healthy = Broker(), RecordingSubscriber()
        for channel in ("news", "other"):
            broker.subscribe(channel, self.connection)
            broker.subscribe(channel, healthy)
        payload = b"x" * (MAX_FIELD_BYTES + 1)
        for _ in range(2):
            broker.subscribe("news", self.connection)
            self.assertEqual(broker.publish("news", payload), 1)
            self.assertEqual(broker.subscribers("news"), 1)
            self.assertEqual(broker.subscribers("other"), 1)
        self.assertEqual(healthy.messages, [("news", payload)] * 2)
        self.transport.write.assert_not_called()
        self.assertEqual(self.transport.abort.call_count, 2)
