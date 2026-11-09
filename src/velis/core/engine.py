"""Execute typed commands against the store and pub-sub broker."""

from typing import assert_never

from velis.core.broker import Broker, Subscriber
from velis.core.messages import (
    NIL,
    OK,
    PERSISTENT,
    TTL,
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
    TTLValue,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.core.store import SWEEP_LIMIT, Store, TtlStatus


def _ok_or_nil(found: bool) -> Ok | Nil:
    return OK if found else NIL


def _ttl_reply(result: int | TtlStatus) -> Nil | Persistent | TTLValue:
    match result:
        case TtlStatus.MISSING:
            return NIL
        case TtlStatus.PERSISTENT:
            return PERSISTENT
        case int(ms):
            return TTLValue(ms)
        case _:
            assert_never(result)


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
                return NIL if value is None else Value(value)
            case Set(key, value):
                self._store.set(key, value)
                return OK
            case Expire(key, ms):
                return _ok_or_nil(self._store.expire(key, ms))
            case Delete(key):
                return _ok_or_nil(self._store.delete(key))
            case Persist(key):
                return _ok_or_nil(self._store.persist(key))
            case TTL(key):
                return _ttl_reply(self._store.ttl(key))
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

    def sweep(self, limit: int = SWEEP_LIMIT) -> int:
        """Run one bounded expiry sweep and return the number of keys removed."""
        return self._store.sweep(limit)

    @property
    def has_due_expirations(self) -> bool:
        """Whether maintenance should continue with another bounded sweep."""
        return self._store.has_due_expirations
