from typing import assert_never

from velis.core.broker import Broker, Subscriber
from velis.core.commands import (
    MISSING,
    NO_EXPIRY,
    Command,
    Delete,
    Delivered,
    Expire,
    Get,
    Nil,
    Ok,
    Persist,
    Persistent,
    Publish,
    Reply,
    Set,
    Subscribe,
    Subscribed,
    Ttl,
    TtlValue,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.core.store import Store


def _ok_or_nil(found: bool) -> Ok | Nil:
    return Ok() if found else Nil()


class Engine:
    """Apply decoded commands to the store and broker and produce replies.

    Store owns TTL and capacity validation; network codecs own field rules.
    One event-loop thread owns the engine, so commands execute without locks.
    The engine has no dependency on sockets or wire formats.
    """

    def __init__(self, store: Store, broker: Broker) -> None:
        self._store = store
        self._broker = broker

    def execute(self, command: Command, subscriber: Subscriber) -> Reply:
        """Execute a command for a subscriber, allowing command errors to propagate."""
        match command:
            case Get(key):
                value = self._store.get(key)
                return Nil() if value is None else Value(value)
            case Set(key, value):
                self._store.set(key, value)
                return Ok()
            case Expire(key, ms):
                return _ok_or_nil(self._store.expire(key, ms))
            case Delete(key):
                return _ok_or_nil(self._store.delete(key))
            case Persist(key):
                return _ok_or_nil(self._store.persist(key))
            case Ttl(key):
                ms = self._store.ttl(key)
                if ms == MISSING:
                    return Nil()
                return Persistent() if ms == NO_EXPIRY else TtlValue(ms)
            case Subscribe(channel):
                self._broker.subscribe(channel, subscriber)
                return Subscribed(channel)
            case Unsubscribe(channel):
                self._broker.unsubscribe(channel, subscriber)
                return Unsubscribed(channel)
            case Publish(channel, payload):
                return Delivered(self._broker.publish(channel, payload))
            case _:
                assert_never(command)

    def disconnect(self, subscriber: Subscriber) -> None:
        """Remove the disconnected subscriber from every channel."""
        self._broker.unsubscribe_all(subscriber)

    def sweep(self) -> int:
        """Run the store expiry sweep and return the number of keys removed."""
        return self._store.sweep()

    @property
    def has_due_expirations(self) -> bool:
        """Whether maintenance should continue with another bounded sweep."""
        return self._store.has_due_expirations
