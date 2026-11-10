"""Typed client commands over opaque bytes, with reply validation."""

from collections.abc import Iterable
from dataclasses import dataclass
from types import TracebackType
from typing import Self, final

from velis.core.messages import (
    Command,
    Err,
    Exists,
    Found,
    Keep,
    KeyPage,
    Lifetime,
    ListKeys,
    Message,
    Nil,
    Ok,
    Ping,
    Pong,
    Read,
    Remaining,
    Remove,
    Reply,
    Send,
    Sent,
    Size,
    SizeValue,
    Unwatch,
    Unwatched,
    Watch,
    Watching,
    Write,
)
from velis.errors import CommandError, ProtocolError
from velis.net.defaults import DEFAULT_HOST, DEFAULT_PORT
from velis.net.replies import expect_reply, remaining_of, unexpected_reply, value_of
from velis.net.session import (
    DEFAULT_SESSION_LIMITS,
    Session,
    SessionLimits,
    TCPSession,
)


@dataclass(frozen=True, slots=True)
class EntrySnapshot:
    """A value and remaining lifetime observed by two consecutive commands.

    This is not an atomic snapshot: time or another client may change the key
    between replies. Missing and persistent lifetimes use MISSING and NO_EXPIRY.
    """

    value: bytes | None
    remaining_ms: int


@final
class Client:
    """Typed cache and pub-sub API over an owned session and opaque byte values."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @classmethod
    async def connect(
        cls,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        *,
        limits: SessionLimits = DEFAULT_SESSION_LIMITS,
    ) -> Self:
        """Create a TCP client with bounded requests and unread messages."""
        session = await TCPSession.connect(host, port, limits=limits)
        return cls(session)

    async def __aenter__(self) -> Self:
        """Use this client within an owned asynchronous resource scope."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the session on both successful and exceptional scope exit."""
        await self.close()

    async def _call[R: Reply](self, command: Command, *accepted: type[R]) -> R:
        """Send a command; raise error replies and reply types other than accepted."""
        reply = await self._session.request(command)
        return expect_reply(type(command).__name__.upper(), reply, *accepted)

    async def _flag(self, command: Command) -> bool:
        """Interpret the common mutation acknowledgment: Ok is True, Nil False."""
        return isinstance(await self._call(command, Ok, Nil), Ok)

    async def _ack(self, command: Command, expected: Reply) -> None:
        """Require the exact acknowledgment, including the echoed channel."""
        reply = await self._call(command, type(expected))
        if reply != expected:
            raise unexpected_reply(type(command).__name__.upper(), reply)

    async def ping(self) -> None:
        """Check server responsiveness; use asyncio.timeout to bound the wait."""
        await self._call(Ping(), Pong)

    async def exists(self, key: str) -> bool:
        """Check live key existence without fetching its value or recording a read."""
        return isinstance(await self._call(Exists(key), Found, Nil), Found)

    async def size(self, key: str) -> int | None:
        """Return live value length in bytes, or None when the key is missing."""
        reply = await self._call(Size(key), SizeValue, Nil)
        return reply.size if isinstance(reply, SizeValue) else None

    async def list_keys(
        self, prefix: str = "", cursor: int = 0, limit: int = 100
    ) -> KeyPage:
        """Read a bounded page in insertion order; zero cursor means traversal end.

        Limit must be 1..100. Pages scan at most 1,000 candidate keys and may be
        empty before traversal ends. Mutations between calls can repeat or omit
        keys because cursors are offsets, not snapshots. No read is recorded.
        """
        return await self._call(ListKeys(prefix, cursor, limit), KeyPage)

    async def read(self, key: str) -> bytes | None:
        """Return stored bytes, or None for a missing or expired key."""
        return value_of(await self._session.request(Read(key)))

    async def write(self, key: str, value: bytes) -> None:
        """Store opaque bytes, clearing any previous TTL."""
        await self._call(Write(key, value), Ok)

    async def lifetime(self, key: str, ms: int) -> bool:
        """Set a TTL in milliseconds; return False if the key is missing."""
        return await self._flag(Lifetime(key, ms))

    async def remove(self, key: str) -> bool:
        """Delete a live key; return whether it existed."""
        return await self._flag(Remove(key))

    async def keep(self, key: str) -> bool:
        """Remove a live key's TTL; return whether it changed."""
        return await self._flag(Keep(key))

    async def remaining(self, key: str) -> int:
        """Remaining milliseconds; MISSING (-2) or NO_EXPIRY (-1) otherwise."""
        return remaining_of(await self._session.request(Remaining(key)))

    async def read_entry(self, key: str) -> EntrySnapshot:
        """Read a value and lifetime in one batch, validating both replies.

        The observations are ordered but not atomic. Server rejections raise
        CommandError and unexpected reply types or counts raise ProtocolError.
        """
        replies = await self.batch((Read(key), Remaining(key)))
        if len(replies) != 2:
            raise ProtocolError("expected two replies to read_entry")
        return EntrySnapshot(value_of(replies[0]), remaining_of(replies[1]))

    async def remaining_many(self, keys: Iterable[str]) -> list[int | CommandError]:
        """Read ordered lifetimes, retaining each server rejection in its slot.

        Missing and persistent keys use MISSING and NO_EXPIRY. Unlike an Err,
        a malformed reply raises ProtocolError because it violates the exchange
        contract. Transport failures propagate; callers receive no partial list.
        """
        commands = tuple(Remaining(key) for key in keys)
        replies = await self.batch(commands)
        if len(replies) != len(commands):
            raise ProtocolError("reply count does not match remaining_many keys")
        return [
            CommandError(reply.message)
            if isinstance(reply, Err)
            else remaining_of(reply)
            for reply in replies
        ]

    async def watch(self, channel: str) -> None:
        """Join a channel to receive server pushes."""
        await self._ack(Watch(channel), Watching(channel))

    async def unwatch(self, channel: str) -> None:
        """Leave a channel without closing the connection."""
        await self._ack(Unwatch(channel), Unwatched(channel))

    async def send(self, channel: str, payload: bytes) -> int:
        """Publish opaque bytes and return the number of accepting subscribers."""
        reply = await self._call(Send(channel, payload), Sent)
        return reply.count

    async def batch(self, commands: Iterable[Command]) -> list[Reply]:
        """Send commands together and return typed replies in input order.

        Unlike individual convenience methods, this returns reply objects,
        including Err for failed commands, so every result remains available.
        TCP sessions validate the whole batch before writing and split it into
        bounded windows when necessary. Batches are not transactions; cancelling
        a batch can leave an already-sent prefix applied on the server.
        """
        return await self._session.batch(commands)

    async def next_message(self) -> Message:
        """Receive a publication with its original payload bytes.

        Use asyncio.timeout to bound the wait. With TCP sessions, cancelling
        this wait leaves the connection usable.
        """
        return await self._session.next_message()

    async def close(self) -> None:
        """Close the owned session and wake waiting operations."""
        await self._session.close()
