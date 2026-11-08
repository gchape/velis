from typing import Protocol

from velis.errors import CapacityError
from velis.validation import require_positive

DEFAULT_MAX_SUBSCRIPTIONS = 1024


class Subscriber(Protocol):
    """Delivery interface implemented by subscriber connections.

    Instances are used as set members and dict keys, so they must be hashable.
    """

    def send(self, channel: str, payload: bytes) -> bool:
        """Accept a message promptly without waiting for delivery or processing.

        Adapters own encoding, buffering and expected transport failures.
        Return False when disconnected or overloaded; the broker then removes
        this subscriber from every channel. Unexpected errors propagate.
        """
        ...


class Broker:
    """Track channel membership and deliver messages to live subscribers.

    Two indexes mirror each other and never keep empty entries: channel to
    subscribers for publishing, and subscriber to channels for limits and cleanup.
    """

    def __init__(self, *, max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS) -> None:
        require_positive("max_subscriptions", max_subscriptions)
        self._max_subscriptions = max_subscriptions
        self._subscribers: dict[str, set[Subscriber]] = {}
        self._subscriptions: dict[Subscriber, set[str]] = {}

    def subscribers(self, channel: str) -> int:
        """Return the number of subscribers currently listening to a channel."""
        return len(self._subscribers.get(channel, ()))

    def subscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Join a channel; repeated subscriptions have no additional effect."""
        channels = self._subscriptions.get(subscriber)
        if channels is not None:
            if channel in channels:
                return
            if len(channels) >= self._max_subscriptions:
                raise CapacityError("subscription limit exceeded")
        self._subscriptions.setdefault(subscriber, set()).add(channel)
        self._subscribers.setdefault(channel, set()).add(subscriber)

    def _drop_member(self, channel: str, subscriber: Subscriber) -> None:
        """Remove a subscriber from a channel's index, pruning it when empty."""
        members = self._subscribers.get(channel)
        if members is not None:
            members.discard(subscriber)
            if not members:
                del self._subscribers[channel]

    def unsubscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Leave a channel, removing empty membership entries."""
        self._drop_member(channel, subscriber)
        channels = self._subscriptions.get(subscriber)
        if channels is not None:
            channels.discard(channel)
            if not channels:
                del self._subscriptions[subscriber]

    def unsubscribe_all(self, subscriber: Subscriber) -> None:
        """Leave every channel; safe to call after earlier cleanup."""
        for channel in self._subscriptions.pop(subscriber, ()):
            self._drop_member(channel, subscriber)

    def publish(self, channel: str, payload: bytes) -> int:
        """Return the number of subscribers that accepted the message.

        Remove failed subscribers from every channel. Acceptance does not
        acknowledge application processing.
        """
        subscribers = self._subscribers.get(channel)
        if not subscribers:
            return 0
        accepted = 0
        for subscriber in list(subscribers):
            if subscriber.send(channel, payload):
                accepted += 1
            else:
                self.unsubscribe_all(subscriber)
        return accepted
