from typing import assert_never

from velis.core.broker import Broker, Subscriber
from velis.core.commands import (
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
from velis.core.expiry import MISSING, NO_EXPIRY
from velis.core.store import Store


def _ok_or_nil(found: bool) -> Ok | Nil:
    return Ok() if found else Nil()


class Engine:
    """Executes commands: validate -> apply -> reply.

    Owned by a single thread (the event loop); no locking needed. Knows
    nothing about sockets or wire formats.
    """

    def __init__(self, store: Store, broker: Broker) -> None:
        self._store = store
        self._broker = broker

    def execute(self, command: Command, subscriber: Subscriber) -> Reply:
        """Validate and execute a command for the requesting subscriber."""
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
