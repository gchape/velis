"""Subscription limits and synchronous message delivery."""

from dataclasses import dataclass
from typing import Protocol

from velis.core.membership import ManyToMany
from velis.core.messages import Message
from velis.errors import CapacityError
from velis.validation import require_positive

DEFAULT_MAX_SUBSCRIPTIONS = 1024


@dataclass(frozen=True, slots=True)
class BrokerLimits:
    """Bound the number of channels each subscriber can join."""

    max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS

    def __post_init__(self) -> None:
        require_positive(max_subscriptions=self.max_subscriptions)


DEFAULT_BROKER_LIMITS = BrokerLimits()


class Subscriber(Protocol):
    """A hashable recipient that accepts messages without waiting for I/O."""

    def send(self, message: Message) -> bool:
        """Return False when disconnected or overloaded; unexpected errors escape."""
        ...


class Broker:
    """Apply subscription limits and deliver one shared message to each peer."""

    def __init__(self, *, limits: BrokerLimits = DEFAULT_BROKER_LIMITS) -> None:
        self._limits = limits
        self._members: ManyToMany[str, Subscriber] = ManyToMany()

    def subscribers(self, channel: str) -> int:
        """Return the number of recipients subscribed to a channel."""
        return self._members.value_count(channel)

    def subscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Join once; enforce the limit without changing existing memberships."""
        if self._members.has(channel, subscriber):
            return
        if self._members.key_count(subscriber) >= self._limits.max_subscriptions:
            raise CapacityError("subscription limit exceeded")
        self._members.add(channel, subscriber)

    def unsubscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Leave a channel."""
        self._members.discard(channel, subscriber)

    def unsubscribe_all(self, subscriber: Subscriber) -> None:
        """Remove all of a subscriber's memberships; safe to repeat."""
        self._members.discard_value(subscriber)

    def publish(self, channel: str, payload: bytes) -> int:
        """Count accepted deliveries, removing failed recipients from every channel."""
        message = Message(channel, payload)
        accepted = 0
        for subscriber in self._members.values_of(channel):
            if subscriber.send(message):
                accepted += 1
            else:
                self.unsubscribe_all(subscriber)
        return accepted
