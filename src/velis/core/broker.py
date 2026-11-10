"""Subscription limits and synchronous message delivery."""

from typing import Protocol, final

from velis.core.membership import Memberships
from velis.core.messages import Message
from velis.errors import CapacityError
from velis.validation import require_positive_int

DEFAULT_MAX_SUBSCRIPTIONS = 1024


class Subscriber(Protocol):
    """A hashable recipient that accepts messages without waiting for I/O."""

    def send(self, message: Message) -> bool:
        """Return False when disconnected or overloaded; unexpected errors escape."""
        ...


@final
class Broker:
    """Deliver each published message to every subscriber of a channel."""

    def __init__(self, *, max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS) -> None:
        require_positive_int(max_subscriptions=max_subscriptions)
        self._max_subscriptions = max_subscriptions
        self._memberships: Memberships[str, Subscriber] = Memberships()

    def subscribers(self, channel: str) -> int:
        """Count current channel members without attempting delivery."""
        return self._memberships.member_count(channel)

    def subscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Join once; the limit applies only to new subscriptions."""
        if self._memberships.is_member(channel, subscriber):
            return
        if self._memberships.group_count(subscriber) >= self._max_subscriptions:
            raise CapacityError("subscription limit exceeded")
        self._memberships.join(channel, subscriber)

    def unsubscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Leave one channel; missing subscriptions are harmless."""
        self._memberships.leave(channel, subscriber)

    def unsubscribe_all(self, subscriber: Subscriber) -> None:
        """Remove all memberships on disconnect or failed delivery."""
        self._memberships.leave_all(subscriber)

    def publish(self, channel: str, payload: bytes) -> int:
        """Return accepted deliveries; remove recipients that return False.

        Iterate a membership snapshot so delivery may change subscriptions.
        Unexpected send exceptions propagate to the caller.
        """
        message = Message(channel, payload)
        accepted = 0
        for subscriber in self._memberships.members(channel):
            if subscriber.send(message):
                accepted += 1
            else:
                self.unsubscribe_all(subscriber)
        return accepted
