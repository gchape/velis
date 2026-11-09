"""Typed client commands, payload transforms and reply validation."""

from types import TracebackType
from typing import Self, final

from velis.codec import Codec, Identity
from velis.core.messages import (
    MISSING,
    NO_EXPIRY,
    TTL,
    Command,
    Delete,
    Delivered,
    Err,
    Expire,
    Get,
    Message,
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
from velis.errors import CommandError, ProtocolError
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol
from velis.net.session import DEFAULT_SESSION_LIMITS, Session, SessionLimits, TCPSession

DEFAULT_CODEC: Codec[bytes, bytes] = Identity()


def _unexpected(command: Command, reply: Reply) -> ProtocolError:
    return ProtocolError(
        f"unexpected reply to {type(command).__name__.upper()}: {reply!r}"
    )


@final
class Client:
    """Typed cache and pub-sub API over an owned session and a byte codec."""

    def __init__(
        self, session: Session, codec: Codec[bytes, bytes] = DEFAULT_CODEC
    ) -> None:
        self._session = session
        self._codec = codec

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        codec: Codec[bytes, bytes] = DEFAULT_CODEC,
        *,
        wire: WireProtocol = DEFAULT_PROTOCOL,
        limits: SessionLimits = DEFAULT_SESSION_LIMITS,
    ) -> Self:
        """Create a TCP client with bounded requests and unread messages."""
        session = await TCPSession.connect(host, port, wire=wire, limits=limits)
        return cls(session, codec)

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
        if isinstance(reply, Err):
            raise CommandError(reply.message)
        if not isinstance(reply, accepted):
            raise _unexpected(command, reply)
        return reply

    async def _flag(self, command: Command) -> bool:
        """Interpret the common mutation acknowledgment: Ok is True, Nil False."""
        return isinstance(await self._call(command, Ok, Nil), Ok)

    async def _ack(self, command: Command, expected: Reply) -> None:
        """Require the exact acknowledgment, including the echoed channel."""
        reply = await self._call(command, type(expected))
        if reply != expected:
            raise _unexpected(command, reply)

    async def get(self, key: str) -> bytes | None:
        """Return the decoded value, or None for a missing or expired key."""
        reply = await self._call(Get(key), Value, Nil)
        return self._codec.decode(reply.value) if isinstance(reply, Value) else None

    async def set(self, key: str, value: bytes) -> None:
        """Encode and store a value, clearing any previous TTL."""
        await self._call(Set(key, self._codec.encode(value)), Ok)

    async def expire(self, key: str, ms: int) -> bool:
        """Set a TTL in milliseconds; return False if the key is missing."""
        return await self._flag(Expire(key, ms))

    async def delete(self, key: str) -> bool:
        """Delete a live key; return whether it existed."""
        return await self._flag(Delete(key))

    async def persist(self, key: str) -> bool:
        """Remove a live key's TTL; return whether it changed."""
        return await self._flag(Persist(key))

    async def ttl(self, key: str) -> int:
        """Remaining milliseconds; MISSING (-2) or NO_EXPIRY (-1) otherwise."""
        match await self._call(TTL(key), TTLValue, Persistent, Nil):
            case TTLValue(ms):
                return ms
            case Persistent():
                return NO_EXPIRY
            case _:
                return MISSING

    async def subscribe(self, channel: str) -> None:
        """Join a channel to receive server pushes."""
        await self._ack(Subscribe(channel), Subscribed(channel))

    async def unsubscribe(self, channel: str) -> None:
        """Leave a channel without closing the connection."""
        await self._ack(Unsubscribe(channel), Unsubscribed(channel))

    async def publish(self, channel: str, payload: bytes) -> int:
        """Encode a message and return the number of accepting subscribers."""
        reply = await self._call(
            Publish(channel, self._codec.encode(payload)), Delivered
        )
        return reply.count

    async def next_message(self) -> Message:
        """Receive a publication and decode its payload with the client codec.

        Use asyncio.timeout to bound the wait. With TCP sessions, cancelling
        this wait leaves the connection usable.
        """
        message = await self._session.next_message()
        return Message(message.channel, self._codec.decode(message.payload))

    async def close(self) -> None:
        """Close the owned session and wake waiting operations."""
        await self._session.close()
