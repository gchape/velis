"""Requests and replies as plain data.

The network layer translates bytes into these values. The engine receives
commands and returns replies. TTL queries use MISSING (-2) and NO_EXPIRY (-1)
when there is no remaining-milliseconds value.
"""

from dataclasses import dataclass

MISSING = -2
NO_EXPIRY = -1


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


@dataclass(frozen=True, slots=True)
class Delete:
    """Delete a live key."""

    key: str


@dataclass(frozen=True, slots=True)
class Ttl:
    """Read a key's remaining TTL in milliseconds."""

    key: str


@dataclass(frozen=True, slots=True)
class Persist:
    """Remove a live key's expiry deadline."""

    key: str


type Command = (
    Get | Set | Expire | Delete | Ttl | Persist | Subscribe | Unsubscribe | Publish
)


@dataclass(frozen=True, slots=True)
class Ok:
    """Acknowledge a successful cache mutation."""


@dataclass(frozen=True, slots=True)
class Nil:
    """Indicate that a key is missing or expired."""


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


@dataclass(frozen=True, slots=True)
class TtlValue:
    """Remaining TTL in milliseconds."""

    ms: int


@dataclass(frozen=True, slots=True)
class Persistent:
    """A live key without an expiry deadline."""


type Reply = (
    Ok
    | Nil
    | Value
    | Err
    | Subscribed
    | Unsubscribed
    | Delivered
    | TtlValue
    | Persistent
)


@dataclass(frozen=True, slots=True)
class Message:
    """Server push to a subscriber (not a reply to any request)."""

    channel: str
    payload: bytes
