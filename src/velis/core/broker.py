"""Subscription limits and synchronous message delivery."""

from typing import Protocol, final

from velis.config import DEFAULT_MAX_SUBSCRIPTIONS
from velis.core.membership import Memberships
from velis.core.messages import Message
from velis.errors import CapacityError
from velis.validation import require_positive


class Subscriber(Protocol):
    """A hashable recipient that accepts messages without waiting for I/O."""

    def send(self, message: Message) -> bool:
        """Return False when disconnected or overloaded; unexpected errors escape."""
        ...


@final
class Broker:
    """Deliver each published message to every subscriber of a channel."""

    def __init__(self, *, max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS) -> None:
        require_positive(max_subscriptions=max_subscriptions)
        self._max_subscriptions = max_subscriptions
        self._memberships: Memberships[str, Subscriber] = Memberships()

    def subscribers(self, channel: str) -> int:
        return self._memberships.member_count(channel)

    def subscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Join once; the limit applies only to new subscriptions."""
        if self._memberships.is_member(channel, subscriber):
            return
        if self._memberships.group_count(subscriber) >= self._max_subscriptions:
            raise CapacityError("subscription limit exceeded")
        self._memberships.join(channel, subscriber)

    def unsubscribe(self, channel: str, subscriber: Subscriber) -> None:
        self._memberships.leave(channel, subscriber)

    def unsubscribe_all(self, subscriber: Subscriber) -> None:
        self._memberships.leave_all(subscriber)

    def publish(self, channel: str, payload: bytes) -> int:
        """Return accepted deliveries; drop subscribers whose send fails."""
        message = Message(channel, payload)
        accepted = 0
        for subscriber in self._memberships.members(channel):
            if subscriber.send(message):
                accepted += 1
            else:
                self.unsubscribe_all(subscriber)
        return accepted
