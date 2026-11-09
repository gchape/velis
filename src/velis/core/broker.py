"""Subscription membership and synchronous publication delivery."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from velis.errors import CapacityError
from velis.validation import require_positive

DEFAULT_MAX_SUBSCRIPTIONS = 1024


@dataclass(frozen=True, slots=True, eq=False)
class Publication:
    """One delivery's immutable data and encodings, cached by wire identity."""

    channel: str
    payload: bytes
    _encodings: dict[int, tuple[object, bytes]] = field(
        default_factory=dict, init=False, repr=False
    )

    def encoded_by(self, wire: object, encode: Callable[[str, bytes], bytes]) -> bytes:
        """Encode once per wire, retaining its identity until this delivery ends."""
        if (entry := self._encodings.get(id(wire))) is None:
            data = encode(self.channel, self.payload)
            self._encodings[id(wire)] = wire, data
            return data
        return entry[1]


class Subscriber(Protocol):
    """A hashable recipient that accepts publications without waiting for I/O."""

    def send(self, publication: Publication) -> bool:
        """Return False when disconnected or overloaded; unexpected errors escape."""
        ...


def _discard[K, V](index: dict[K, set[V]], key: K, item: V) -> None:
    if (items := index.get(key)) is not None:
        items.discard(item)
        if not items:
            del index[key]


class _Membership[K, V]:
    """A many-to-many relation whose mirrored indexes have no empty entries."""

    def __init__(self) -> None:
        self._by_key: dict[K, set[V]] = {}
        self._by_item: dict[V, set[K]] = {}

    def add(self, key: K, item: V) -> None:
        self._by_key.setdefault(key, set()).add(item)
        self._by_item.setdefault(item, set()).add(key)

    def discard(self, key: K, item: V) -> None:
        _discard(self._by_key, key, item)
        _discard(self._by_item, item, key)

    def discard_item(self, item: V) -> None:
        for key in self._by_item.pop(item, ()):
            _discard(self._by_key, key, item)

    def contains(self, key: K, item: V) -> bool:
        return item in self._by_key.get(key, ())

    def key_count(self, key: K) -> int:
        return len(self._by_key.get(key, ()))

    def item_count(self, item: V) -> int:
        return len(self._by_item.get(item, ()))

    def items_for(self, key: K) -> tuple[V, ...]:
        return tuple(self._by_key.get(key, ()))


class Broker:
    """Apply subscription limits and deliver one shared publication to each peer."""

    def __init__(self, *, max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS) -> None:
        require_positive(max_subscriptions=max_subscriptions)
        self._max_subscriptions = max_subscriptions
        self._members: _Membership[str, Subscriber] = _Membership()

    def subscribers(self, channel: str) -> int:
        """Return the number of recipients subscribed to a channel."""
        return self._members.key_count(channel)

    def subscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Join once; enforce the limit without changing existing memberships."""
        if self._members.contains(channel, subscriber):
            return
        if self._members.item_count(subscriber) >= self._max_subscriptions:
            raise CapacityError("subscription limit exceeded")
        self._members.add(channel, subscriber)

    def unsubscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Leave a channel and prune both indexes when they become empty."""
        self._members.discard(channel, subscriber)

    def unsubscribe_all(self, subscriber: Subscriber) -> None:
        """Remove all of a subscriber's memberships; safe to repeat."""
        self._members.discard_item(subscriber)

    def publish(self, channel: str, payload: bytes) -> int:
        """Count accepted deliveries, removing failed recipients from every channel."""
        publication = Publication(channel, payload)
        accepted = 0
        for subscriber in self._members.items_for(channel):
            if subscriber.send(publication):
                accepted += 1
            else:
                self.unsubscribe_all(subscriber)
        return accepted
