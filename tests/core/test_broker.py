import unittest
from unittest.mock import Mock

from tests.helpers import RecordingSubscriber
from velis.core.broker import Broker, BrokerLimits, Subscriber
from velis.core.messages import Message
from velis.errors import CapacityError


class BrokerTests(unittest.TestCase):
    def test_publish_shares_one_message_across_subscribers(self) -> None:
        broker = Broker()
        first, second = Mock(spec=Subscriber), Mock(spec=Subscriber)
        for subscriber in (first, second):
            subscriber.send.return_value = True
            broker.subscribe("news", subscriber)
        self.assertEqual(broker.publish("news", b"hello"), 2)
        message = first.send.call_args.args[0]
        self.assertEqual(message, Message("news", b"hello"))
        self.assertIs(second.send.call_args.args[0], message)

    def test_subscription_limit_preserves_membership_and_allows_reuse(self) -> None:
        broker = Broker(limits=BrokerLimits(max_subscriptions=1))
        first, second = RecordingSubscriber(), RecordingSubscriber()
        broker.subscribe("a", first)
        broker.subscribe("a", first)
        broker.subscribe("b", second)
        with self.assertRaisesRegex(CapacityError, "subscription limit"):
            broker.subscribe("b", first)
        self.assertEqual(broker.publish("a", b"first"), 1)
        self.assertEqual(broker.publish("b", b"second"), 1)
        broker.unsubscribe("a", first)
        broker.subscribe("b", first)
        self.assertEqual(broker.publish("b", b"both"), 2)
        broker.unsubscribe_all(first)
        broker.subscribe("c", first)
        self.assertEqual(broker.subscribers("c"), 1)

    def test_subscription_limit_must_be_positive(self) -> None:
        for limit in (0, -1):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                BrokerLimits(max_subscriptions=limit)

    def test_publish_counts_deliveries(self) -> None:
        b, a1, a2 = Broker(), RecordingSubscriber(), RecordingSubscriber()
        b.subscribe("c", a1)
        b.subscribe("c", a2)
        self.assertEqual(b.publish("c", b"hi"), 2)
        self.assertEqual(a1.messages, [("c", b"hi")])

    def test_no_subscribers(self) -> None:
        self.assertEqual(Broker().publish("c", b"x"), 0)

    def test_dead_subscriber_is_dropped_everywhere_and_not_counted(self) -> None:
        """A failed delivery removes the subscriber from every joined channel."""
        b, live, dead = Broker(), RecordingSubscriber(), RecordingSubscriber()
        for ch in ("a", "b"):
            b.subscribe(ch, live)
            b.subscribe(ch, dead)
        dead.alive = False
        self.assertEqual(b.publish("a", b"x"), 1)
        self.assertEqual(b.subscribers("a"), 1)
        self.assertEqual(b.subscribers("b"), 1)
        self.assertEqual(live.messages, [("a", b"x")])
        self.assertEqual(dead.messages, [])
        self.assertEqual(b.publish("b", b"next"), 1)
        self.assertEqual(live.messages, [("a", b"x"), ("b", b"next")])

    def test_unsubscribe_and_unsubscribe_all(self) -> None:
        """Leaving channels and repeating cleanup is safe."""
        b, s = Broker(), RecordingSubscriber()
        b.subscribe("a", s)
        b.subscribe("b", s)
        b.unsubscribe("a", s)
        self.assertEqual(b.subscribers("a"), 0)
        b.unsubscribe_all(s)
        self.assertEqual(b.subscribers("b"), 0)
        b.unsubscribe_all(s)


if __name__ == "__main__":
    unittest.main()
