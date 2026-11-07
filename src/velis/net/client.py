from __future__ import annotations

from types import TracebackType

from velis.codec import Codec, CodecChain
from velis.core.commands import (
    Command,
    Delivered,
    Err,
    Expire,
    Get,
    Message,
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
from velis.errors import CommandError, ProtocolError
from velis.net.protocol import DEFAULT_PROTOCOL, WireProtocol
from velis.net.session import Session, TcpSession


class Client:
    """Typed cache and pub-sub API over an owned session and a byte codec."""

    def __init__(
        self, session: Session, codec: Codec[bytes, bytes] | None = None
    ) -> None:
        self._session = session
        self._codec: Codec[bytes, bytes] = (
            CodecChain.identity(bytes) if codec is None else codec
        )

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        codec: Codec[bytes, bytes] | None = None,
        *,
        wire: WireProtocol = DEFAULT_PROTOCOL,
        max_pending_messages: int = 1024,
        max_pending_bytes: int = 8 << 20,
    ) -> Client:
        """Create a client backed by a TCP session with bounded unread messages."""
        session = await TcpSession.connect(
            host,
            port,
            wire=wire,
            max_pending_messages=max_pending_messages,
            max_pending_bytes=max_pending_bytes,
        )
        try:
            return cls(session, codec)
        except Exception:
            await session.close()
            raise

    async def __aenter__(self) -> Client:
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

    async def _request(self, command: Command) -> Reply:
        """Translate application error replies at the client API boundary."""
        reply = await self._session.request(command)
        if isinstance(reply, Err):
            raise CommandError(reply.message)
        return reply

    async def get(self, key: str) -> bytes | None:
        """Return the decoded value, or None for a missing or expired key."""
        match await self._request(Get(key)):
            case Value(value):
                return self._codec.decode(value)
            case Nil():
                return None
            case other:
                raise ProtocolError(f"unexpected reply to GET: {other!r}")

    async def set(self, key: str, value: bytes) -> None:
        """Encode and store a value, clearing any previous TTL."""
        await self._expect_reply(Set(key, self._codec.encode(value)), Ok())

    async def expire(self, key: str, ms: int) -> bool:
        """Set a TTL in milliseconds; return False if the key is missing."""
        match await self._request(Expire(key, ms)):
            case Ok():
                return True
            case Nil():
                return False
            case other:
                raise ProtocolError(f"unexpected reply to EXPIRE: {other!r}")

    async def subscribe(self, channel: str) -> None:
        """Join a channel to receive server pushes."""
        await self._expect_reply(Subscribe(channel), Subscribed(channel))

    async def unsubscribe(self, channel: str) -> None:
        """Leave a channel without closing the connection."""
        await self._expect_reply(Unsubscribe(channel), Unsubscribed(channel))

    async def publish(self, channel: str, payload: bytes) -> int:
        """Encode a message and return the number of accepting subscribers."""
        match await self._request(Publish(channel, self._codec.encode(payload))):
            case Delivered(count):
                return count
            case other:
                raise ProtocolError(f"unexpected reply to PUBLISH: {other!r}")

    async def next_message(self) -> Message:
        """Receive a publication and decode its payload with the client codec.

        Use asyncio.timeout to bound the wait. With TCP sessions, cancelling
        this wait leaves the connection usable.
        """
        message = await self._session.next_message()
        return Message(message.channel, self._codec.decode(message.payload))

    async def _expect_reply(self, command: Command, expected: Reply) -> None:
        """Validate acknowledgments, including the channel for subscriptions."""
        reply = await self._request(command)
        if reply != expected:
            name = type(command).__name__.upper()
            raise ProtocolError(f"unexpected reply to {name}: {reply!r}")

    async def close(self) -> None:
        """Close the owned session and wake waiting operations."""
        await self._session.close()
