import random
import unittest
from unittest.mock import Mock

from tests.helpers import RecordingSubscriber
from velis.core.broker import Broker, Publication, _Membership
from velis.errors import CapacityError, ProtocolError


class PublicationTests(unittest.TestCase):
    def test_cache_uses_identity_and_retains_empty_encodings(self) -> None:
        first: list[str] = []
        second: list[str] = []
        encode = Mock(side_effect=[b"", b"second"])
        publication = Publication("news", b"hello")
        for _ in range(2):
            self.assertEqual(publication.encoded_by(first, encode), b"")
        for _ in range(2):
            self.assertEqual(publication.encoded_by(second, encode), b"second")
        self.assertEqual(encode.call_count, 2)
        encode.assert_called_with("news", b"hello")

    def test_failed_encoding_does_not_poison_the_cache(self) -> None:
        wire = object()
        encode = Mock(side_effect=[ProtocolError("invalid"), b"ok"])
        publication = Publication("news", b"hello")
        with self.assertRaises(ProtocolError):
            publication.encoded_by(wire, encode)
        self.assertEqual(publication.encoded_by(wire, encode), b"ok")
        self.assertEqual(publication.encoded_by(wire, encode), b"ok")
        self.assertEqual(encode.call_count, 2)


class MembershipTests(unittest.TestCase):
    def test_mirrored_indexes_match_a_set_of_pairs_and_prune_empty_entries(
        self,
    ) -> None:
        rng = random.Random(47)
        membership: _Membership[int, int] = _Membership()
        pairs: set[tuple[int, int]] = set()
        for step in range(1000):
            key, item = rng.randrange(8), rng.randrange(8)
            match rng.randrange(3):
                case 0:
                    membership.add(key, item)
                    pairs.add((key, item))
                case 1:
                    membership.discard(key, item)
                    pairs.discard((key, item))
                case 2:
                    membership.discard_item(item)
                    pairs = {(k, v) for k, v in pairs if v != item}
            with self.subTest(step=step):
                self.assertEqual(set(membership._by_key), {k for k, _ in pairs})
                self.assertEqual(set(membership._by_item), {v for _, v in pairs})
                for candidate in range(8):
                    self.assertEqual(
                        set(membership.items_for(candidate)),
                        {v for k, v in pairs if k == candidate},
                    )
                    self.assertEqual(
                        membership.item_count(candidate),
                        sum(v == candidate for _, v in pairs),
                    )


class BrokerTests(unittest.TestCase):
    def test_subscription_limit_preserves_membership_and_allows_reuse(self) -> None:
        broker = Broker(max_subscriptions=1)
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
                Broker(max_subscriptions=limit)

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
