from __future__ import annotations

from collections import defaultdict
from typing import Protocol


class Subscriber(Protocol):
    """Delivery interface implemented by subscriber connections."""

    def send(self, channel: str, payload: bytes) -> bool:
        """Deliver one message. Return False if this subscriber is gone or
        too slow; the broker then drops it from every channel."""
        ...


class Broker:
    """Track channel membership and deliver messages to live subscribers."""

    def __init__(self) -> None:
        self._channels: dict[str, set[Subscriber]] = defaultdict(set)
        self._joined: dict[Subscriber, set[str]] = defaultdict(set)

    def subscribers(self, channel: str) -> int:
        """Return the number of subscribers currently joined to a channel."""
        return len(self._channels.get(channel, ()))

    def subscribe(self, channel: str, sub: Subscriber) -> None:
        """Join a channel; repeated subscriptions have no additional effect."""
        self._joined[sub].add(channel)
        self._channels[channel].add(sub)

    def unsubscribe(self, channel: str, sub: Subscriber) -> None:
        """Leave a channel, removing empty membership entries."""
        subs = self._channels.get(channel)
        if subs is not None:
            subs.discard(sub)
            if not subs:
                del self._channels[channel]
        joined = self._joined.get(sub)
        if joined is not None:
            joined.discard(channel)
            if not joined:
                del self._joined[sub]

    def unsubscribe_all(self, sub: Subscriber) -> None:
        """Leave every channel; safe to call after earlier cleanup."""
        for channel in list(self._joined.get(sub, ())):
            self.unsubscribe(channel, sub)

    def publish(self, channel: str, payload: bytes) -> int:
        """Return the number of subscribers that accepted the message.

        Remove failed subscribers from every channel. Acceptance does not
        acknowledge application processing.
        """
        subs = self._channels.get(channel)
        if not subs:
            return 0
        delivered = 0
        for sub in list(subs):
            if sub.send(channel, payload):
                delivered += 1
            else:
                self.unsubscribe_all(sub)
        return delivered
