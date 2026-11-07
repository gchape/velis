from __future__ import annotations

from collections import defaultdict
from typing import Protocol


class Subscriber(Protocol):
    """Delivery interface implemented by subscriber connections."""

    def send(self, channel: str, payload: bytes) -> bool:
        """Accept a message promptly without waiting for delivery or processing.

        Adapters own encoding, buffering and expected transport failures.
        Return False when disconnected or overloaded; the broker then removes
        this subscriber from every channel. Unexpected errors propagate.
        """
        ...


class Broker:
    """Track channel membership and deliver messages to live subscribers."""

    def __init__(self) -> None:
        self._subscribers: dict[str, set[Subscriber]] = defaultdict(set)
        self._subscriptions: dict[Subscriber, set[str]] = defaultdict(set)

    def subscribers(self, channel: str) -> int:
        """Return the number of subscribers currently listening to a channel."""
        return len(self._subscribers.get(channel, ()))

    def subscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Join a channel; repeated subscriptions have no additional effect."""
        self._subscriptions[subscriber].add(channel)
        self._subscribers[channel].add(subscriber)

    def unsubscribe(self, channel: str, subscriber: Subscriber) -> None:
        """Leave a channel, removing empty membership entries."""
        subscribers = self._subscribers.get(channel)
        if subscribers is not None:
            subscribers.discard(subscriber)
            if not subscribers:
                del self._subscribers[channel]
        channels = self._subscriptions.get(subscriber)
        if channels is not None:
            channels.discard(channel)
            if not channels:
                del self._subscriptions[subscriber]

    def unsubscribe_all(self, subscriber: Subscriber) -> None:
        """Leave every channel; safe to call after earlier cleanup."""
        for channel in list(self._subscriptions.get(subscriber, ())):
            self.unsubscribe(channel, subscriber)

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
