"""Commands, replies and publication pushes as plain message data.

The network layer translates bytes into these values. The engine receives
commands and returns replies. Client.remaining() uses MISSING (-2) and NO_EXPIRY (-1)
when there is no remaining-milliseconds value; Store.ttl() uses TtlStatus.
The payload-free reply instances OK, NIL and UNLIMITED are immutable and shared.
"""

from dataclasses import dataclass
from typing import Final

MISSING: Final = -2
NO_EXPIRY: Final = -1


@dataclass(frozen=True, slots=True)
class Read:
    """Read a live value by key."""

    key: str


@dataclass(frozen=True, slots=True)
class Write:
    """Write opaque bytes and clear the key TTL."""

    key: str
    value: bytes


@dataclass(frozen=True, slots=True)
class Lifetime:
    """Set a key TTL in milliseconds."""

    key: str
    ms: int


@dataclass(frozen=True, slots=True)
class Watch:
    """Join a pub-sub channel."""

    channel: str


@dataclass(frozen=True, slots=True)
class Unwatch:
    """Leave a pub-sub channel."""

    channel: str


@dataclass(frozen=True, slots=True)
class Send:
    """Send opaque bytes to the current subscribers of a channel."""

    channel: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class Remove:
    """Delete a live key."""

    key: str


@dataclass(frozen=True, slots=True)
class Remaining:
    """Read a key's remaining TTL in milliseconds."""

    key: str


@dataclass(frozen=True, slots=True)
class Keep:
    """Remove a live key's expiry deadline."""

    key: str


@dataclass(frozen=True, slots=True)
class Ping:
    """Check that the server can answer a command."""


@dataclass(frozen=True, slots=True)
class Exists:
    """Inspect key existence without reading its value or updating eviction."""

    key: str


@dataclass(frozen=True, slots=True)
class Size:
    """Inspect the byte length of a live value without fetching its content."""

    key: str


@dataclass(frozen=True, slots=True)
class ListKeys:
    """Request one bounded, weakly consistent page of matching live keys."""

    prefix: str = ""
    cursor: int = 0
    limit: int = 100


type Command = (
    Read
    | Write
    | Lifetime
    | Remove
    | Remaining
    | Keep
    | Watch
    | Unwatch
    | Send
    | Ping
    | Exists
    | Size
    | ListKeys
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
class Watching:
    """Acknowledge channel subscription."""

    channel: str


@dataclass(frozen=True, slots=True)
class Unwatched:
    """Acknowledge channel unsubscription."""

    channel: str


@dataclass(frozen=True, slots=True)
class Sent:
    """Report how many subscriber connections accepted a message."""

    count: int


@dataclass(frozen=True, slots=True)
class RemainingValue:
    """Remaining TTL in milliseconds."""

    ms: int


@dataclass(frozen=True, slots=True)
class Unlimited:
    """A live key without an expiry deadline."""


@dataclass(frozen=True, slots=True)
class Pong:
    """Acknowledge that the server is responsive."""


@dataclass(frozen=True, slots=True)
class Found:
    """Confirm that a key currently exists."""


@dataclass(frozen=True, slots=True)
class SizeValue:
    """Return the byte length of a stored value."""

    size: int


@dataclass(frozen=True, slots=True)
class KeyPage:
    """Return keys and the next cursor; zero marks the end of this traversal.

    Cursors are offsets into current insertion order, not snapshot tokens.
    Intervening mutations can cause repeated or omitted keys. An empty page
    can have a nonzero cursor when the candidate scan budget was exhausted.
    """

    cursor: int
    keys: tuple[str, ...]


type Reply = (
    Ok
    | Nil
    | Value
    | Err
    | Watching
    | Unwatched
    | Sent
    | RemainingValue
    | Unlimited
    | Pong
    | Found
    | SizeValue
    | KeyPage
)

OK: Final = Ok()
NIL: Final = Nil()
UNLIMITED: Final = Unlimited()


@dataclass(frozen=True, slots=True)
class Message:
    """Server push to a subscriber (not a reply to any request)."""

    channel: str
    payload: bytes
