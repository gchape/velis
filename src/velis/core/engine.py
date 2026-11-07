from __future__ import annotations

from typing import assert_never

from velis.core.broker import Broker, Subscriber
from velis.core.commands import (
    Command,
    Delivered,
    Expire,
    Get,
    Nil,
    Ok,
    Publish,
    Reply,
    Set,
    Subscribe,
    Subscribed,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.core.store import Store
from velis.errors import CommandError

MAX_TTL_MS = 1 << 62


class Engine:
    """Executes commands: validate -> apply -> reply.

    Owned by a single thread (the event loop); no locking needed. Knows
    nothing about sockets or wire formats.
    """

    def __init__(self, store: Store, broker: Broker) -> None:
        self._store = store
        self._broker = broker


    def execute(self, cmd: Command, sub: Subscriber) -> Reply:
        """Validate and execute a command for the requesting subscriber."""
        match cmd:
            case Get(key):
                value = self._store.get(key)
                return Nil() if value is None else Value(value)
            case Set(key, value):
                self._store.set(key, value)
                return Ok()
            case Expire(key, ms):
                if not 0 <= ms <= MAX_TTL_MS:
                    raise CommandError("ttl out of range")
                return Ok() if self._store.expire(key, ms) else Nil()
            case Subscribe(channel):
                self._broker.subscribe(channel, sub)
                return Subscribed(channel)
            case Unsubscribe(channel):
                self._broker.unsubscribe(channel, sub)
                return Unsubscribed(channel)
            case Publish(channel, payload):
                return Delivered(self._broker.publish(channel, payload))
            case _:
                assert_never(cmd)

    def disconnect(self, sub: Subscriber) -> None:
        """Remove the disconnected subscriber from every channel."""
        self._broker.unsubscribe_all(sub)


    def sweep(self) -> int:
        """Run the store expiry sweep and return the number of keys removed."""
        return self._store.sweep()
