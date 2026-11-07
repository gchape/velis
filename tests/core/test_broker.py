import unittest

from tests.helpers import RecordingSubscriber
from velis.core.broker import Broker


class BrokerTests(unittest.TestCase):
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
