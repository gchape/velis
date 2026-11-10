"""Execute typed commands against the store and pub-sub broker."""

from typing import assert_never

from velis.core.broker import Broker, Subscriber
from velis.core.messages import (
    NIL,
    OK,
    UNLIMITED,
    Command,
    Exists,
    Found,
    Keep,
    KeyPage,
    Lifetime,
    ListKeys,
    Nil,
    Ok,
    Ping,
    Pong,
    Read,
    Remaining,
    RemainingValue,
    Remove,
    Reply,
    Send,
    Sent,
    Size,
    SizeValue,
    Unlimited,
    Unwatch,
    Unwatched,
    Value,
    Watch,
    Watching,
    Write,
)
from velis.core.store import SWEEP_LIMIT, Store, TtlStatus


def _ok_or_nil(found: bool) -> Ok | Nil:
    return OK if found else NIL


def _ttl_reply(result: int | TtlStatus) -> Nil | Unlimited | RemainingValue:
    match result:
        case TtlStatus.MISSING:
            return NIL
        case TtlStatus.PERSISTENT:
            return UNLIMITED
        case int(ms):
            return RemainingValue(ms)
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
        """Execute a typed command, allowing validation errors to propagate."""
        if isinstance(command, Ping | Exists | Size | ListKeys):
            return self._inspect(command)
        return self._execute(command, subscriber)

    def _inspect(self, command: Ping | Exists | Size | ListKeys) -> Reply:
        """Inspect metadata without fetching values or touching eviction order."""
        match command:
            case Ping():
                return Pong()
            case Exists(key):
                return Found() if self._store.exists(key) else NIL
            case Size(key):
                size = self._store.size(key)
                return NIL if size is None else SizeValue(size)
            case ListKeys(prefix, cursor, limit):
                next_cursor, keys = self._store.list_keys(prefix, cursor, limit)
                return KeyPage(next_cursor, keys)
            case _:
                assert_never(command)

    def _execute(
        self,
        command: Read
        | Write
        | Lifetime
        | Remove
        | Remaining
        | Keep
        | Watch
        | Unwatch
        | Send,
        subscriber: Subscriber,
    ) -> Reply:
        """Execute a command for a subscriber, allowing command errors to propagate."""
        match command:
            case Read(key):
                value = self._store.get(key)
                return NIL if value is None else Value(value)
            case Write(key, value):
                self._store.set(key, value)
                return OK
            case Lifetime(key, ms):
                return _ok_or_nil(self._store.expire(key, ms))
            case Remove(key):
                return _ok_or_nil(self._store.delete(key))
            case Keep(key):
                return _ok_or_nil(self._store.persist(key))
            case Remaining(key):
                return _ttl_reply(self._store.ttl(key))
            case Watch(channel):
                self._broker.subscribe(channel, subscriber)
                return Watching(channel)
            case Unwatch(channel):
                self._broker.unsubscribe(channel, subscriber)
                return Unwatched(channel)
            case Send(channel, payload):
                return Sent(self._broker.publish(channel, payload))
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
