import unittest
from unittest.mock import Mock

from tests.helpers import RecordingSubscriber
from velis.core.broker import Broker
from velis.core.messages import Message
from velis.errors import CapacityError


class BrokerTests(unittest.TestCase):
    def test_duplicate_subscriptions_and_missing_unsubscribe_are_idempotent(
        self,
    ) -> None:
        broker = Broker(max_subscriptions=1)
        subscriber = RecordingSubscriber()
        broker.subscribe("a", subscriber)
        broker.subscribe("a", subscriber)
        broker.unsubscribe("missing", subscriber)
        self.assertEqual(broker.subscribers("a"), 1)
        self.assertEqual(broker.publish("a", b"\xff"), 1)
        self.assertEqual(subscriber.messages, [Message("a", b"\xff")])
        broker.unsubscribe("a", subscriber)
        broker.unsubscribe("a", subscriber)
        self.assertEqual(broker.publish("a", b"ignored"), 0)

    def test_limit_is_per_subscriber_and_leaving_frees_a_slot(self) -> None:
        broker = Broker(max_subscriptions=1)
        first, second = RecordingSubscriber(), RecordingSubscriber()
        broker.subscribe("a", first)
        broker.subscribe("b", second)
        with self.assertRaisesRegex(CapacityError, "subscription limit"):
            broker.subscribe("b", first)
        self.assertEqual(broker.subscribers("b"), 1)
        broker.unsubscribe("a", first)
        broker.subscribe("b", first)
        self.assertEqual(broker.subscribers("b"), 2)

    def test_failed_delivery_removes_subscriber_from_every_channel(self) -> None:
        broker = Broker()
        healthy = RecordingSubscriber()
        overloaded = RecordingSubscriber(accepting=False)
        for subscriber in (healthy, overloaded):
            broker.subscribe("a", subscriber)
            broker.subscribe("b", subscriber)
        self.assertEqual(broker.publish("a", b"one"), 1)
        self.assertEqual(broker.subscribers("a"), 1)
        self.assertEqual(broker.subscribers("b"), 1)
        self.assertIs(healthy.messages[0], overloaded.messages[0])
        self.assertEqual(broker.publish("b", b"two"), 1)
        self.assertEqual(len(overloaded.messages), 1)

    def test_delivery_snapshot_allows_subscriber_to_remove_itself(self) -> None:
        broker = Broker()
        first, second = Mock(), RecordingSubscriber()
        first.send.side_effect = lambda message: broker.unsubscribe_all(first) or True
        broker.subscribe("a", first)
        broker.subscribe("a", second)
        self.assertEqual(broker.publish("a", b"v"), 2)
        self.assertEqual(broker.subscribers("a"), 1)
        self.assertEqual(second.messages, [Message("a", b"v")])

    def test_unexpected_subscriber_failures_escape(self) -> None:
        broker = Broker()
        subscriber = Mock()
        subscriber.send.side_effect = RuntimeError("subscriber failed")
        broker.subscribe("a", subscriber)
        with self.assertRaisesRegex(RuntimeError, "subscriber failed"):
            broker.publish("a", b"v")

    def test_subscription_limit_must_be_positive(self) -> None:
        for limit in (0, -1):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                Broker(max_subscriptions=limit)
