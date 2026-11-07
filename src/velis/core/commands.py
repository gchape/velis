"""Requests and replies as plain data.

The net layer turns bytes into these and back; the Engine only ever sees
these.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Get:
    """Read a live value by key."""

    key: str


@dataclass(frozen=True, slots=True)
class Set:
    """Write opaque bytes and clear the key TTL."""

    key: str
    value: bytes


@dataclass(frozen=True, slots=True)
class Expire:
    """Set a key TTL in milliseconds."""

    key: str
    ms: int


@dataclass(frozen=True, slots=True)
class Subscribe:
    """Join a pub-sub channel."""

    channel: str


@dataclass(frozen=True, slots=True)
class Unsubscribe:
    """Leave a pub-sub channel."""

    channel: str


@dataclass(frozen=True, slots=True)
class Publish:
    """Send opaque bytes to the current subscribers of a channel."""

    channel: str
    payload: bytes


type Command = Get | Set | Expire | Subscribe | Unsubscribe | Publish


@dataclass(frozen=True, slots=True)
class Ok:
    """Acknowledge a successful cache mutation."""

    ...


@dataclass(frozen=True, slots=True)
class Nil:
    """Indicate that a key is missing or expired."""

    ...


@dataclass(frozen=True, slots=True)
class Value:
    """Return the opaque bytes stored under a key."""

    value: bytes


@dataclass(frozen=True, slots=True)
class Err:
    """Describe a command or protocol error."""

    message: str


@dataclass(frozen=True, slots=True)
class Subscribed:
    """Acknowledge channel subscription."""

    channel: str


@dataclass(frozen=True, slots=True)
class Unsubscribed:
    """Acknowledge channel unsubscription."""

    channel: str


@dataclass(frozen=True, slots=True)
class Delivered:
    """Report how many subscriber connections accepted a message."""

    count: int


type Reply = Ok | Nil | Value | Err | Subscribed | Unsubscribed | Delivered


@dataclass(frozen=True, slots=True)
class Message:
    """Server push to a subscriber (not a reply to any request)."""

    channel: str
    payload: bytes
