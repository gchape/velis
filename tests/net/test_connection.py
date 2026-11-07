import asyncio
import unittest
from unittest.mock import Mock

from tests.helpers import RecordingSubscriber
from velis.core.broker import Broker
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.connection import Connection


class ConnectionDeliveryTests(unittest.TestCase):
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

    def test_encoding_errors_are_not_treated_as_disconnections(self) -> None:
        with self.assertRaises(ProtocolError):
            self.connection.send("news", b"x" * (protocol.MAX_LEN + 1))
        self.writer.write.assert_not_called()
        self.writer.transport.abort.assert_not_called()
