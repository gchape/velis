import unittest
from unittest.mock import AsyncMock, Mock, patch

from velis.config import SessionLimits
from velis.core.messages import (
    MISSING,
    NO_EXPIRY,
    TTL,
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
    Set,
    Subscribe,
    Subscribed,
    TTLValue,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.errors import CommandError, ProtocolError
from velis.net.client import Client
from velis.net.session import Session


class ClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = Mock(spec=Session)
        self.session.request = AsyncMock()
        self.session.next_message = AsyncMock()
        self.session.close = AsyncMock()
        self.client = Client(self.session)

    async def test_commands_and_reply_values(self) -> None:
        cases = (
            ("get", ("k",), Get("k"), Value(b"\x00\xff"), b"\x00\xff"),
            ("get", ("k",), Get("k"), Nil(), None),
            ("set", ("k", b"v"), Set("k", b"v"), Ok(), None),
            ("expire", ("k", 12), Expire("k", 12), Ok(), True),
            ("expire", ("k", 12), Expire("k", 12), Nil(), False),
            ("delete", ("k",), Delete("k"), Ok(), True),
            ("delete", ("k",), Delete("k"), Nil(), False),
            ("persist", ("k",), Persist("k"), Ok(), True),
            ("persist", ("k",), Persist("k"), Nil(), False),
            ("ttl", ("k",), TTL("k"), Nil(), MISSING),
            ("ttl", ("k",), TTL("k"), Persistent(), NO_EXPIRY),
            ("ttl", ("k",), TTL("k"), TTLValue(12), 12),
            ("subscribe", ("c",), Subscribe("c"), Subscribed("c"), None),
            ("unsubscribe", ("c",), Unsubscribe("c"), Unsubscribed("c"), None),
            ("publish", ("c", b"v"), Publish("c", b"v"), Delivered(3), 3),
        )
        for method, args, command, reply, expected in cases:
            with self.subTest(command=command, reply=reply):
                self.session.request.reset_mock()
                self.session.request.return_value = reply
                self.assertEqual(await getattr(self.client, method)(*args), expected)
                self.session.request.assert_awaited_once_with(command)

    async def test_connect_defaults_and_limit_overrides_reach_the_session(self) -> None:
        limits = SessionLimits(max_pending_requests=3)
        with patch(
            "velis.net.client.TCPSession.connect", return_value=self.session
        ) as connect:
            async with await Client.connect(limits=limits):
                connect.assert_awaited_once_with("127.0.0.1", 9000, limits=limits)
        self.session.close.assert_awaited_once_with()

    async def test_server_errors_become_command_errors_for_every_api(self) -> None:
        for method, args in (
            ("get", ("k",)),
            ("set", ("k", b"v")),
            ("expire", ("k", 1)),
            ("delete", ("k",)),
            ("persist", ("k",)),
            ("ttl", ("k",)),
            ("subscribe", ("c",)),
            ("unsubscribe", ("c",)),
            ("publish", ("c", b"v")),
        ):
            with self.subTest(method=method):
                self.session.request.return_value = Err("rejected")
                with self.assertRaisesRegex(CommandError, "rejected"):
                    await getattr(self.client, method)(*args)

    async def test_unexpected_reply_types_and_wrong_echoed_channels_are_rejected(
        self,
    ) -> None:
        for method, args, reply in (
            ("get", ("k",), Ok()),
            ("set", ("k", b"v"), Nil()),
            ("expire", ("k", 1), Value(b"v")),
            ("delete", ("k",), Persistent()),
            ("persist", ("k",), TTLValue(1)),
            ("ttl", ("k",), Ok()),
            ("subscribe", ("c",), Subscribed("wrong")),
            ("unsubscribe", ("c",), Unsubscribed("wrong")),
            ("publish", ("c", b"v"), Ok()),
        ):
            with self.subTest(method=method, reply=reply):
                self.session.request.return_value = reply
                with self.assertRaisesRegex(ProtocolError, "unexpected reply"):
                    await getattr(self.client, method)(*args)

    async def test_next_message_delegates_without_changing_payload(self) -> None:
        message = Message("c", b"\x00\xff")
        self.session.next_message.return_value = message
        self.assertIs(await self.client.next_message(), message)

    async def test_batch_delegates_and_preserves_error_reply_positions(self) -> None:
        commands = [Set("k", b"v"), Get("k"), Expire("k", 1)]
        replies = [Ok(), Value(b"v"), Err("rejected")]
        self.session.batch.return_value = replies
        self.assertIs(await self.client.batch(commands), replies)
        self.session.batch.assert_awaited_once_with(commands)
        self.session.request.assert_not_awaited()

    async def test_batch_propagates_transport_failures(self) -> None:
        self.session.batch.side_effect = ConnectionError("batch failed")
        with self.assertRaisesRegex(ConnectionError, "batch failed"):
            await self.client.batch([Get("k")])

    async def test_context_manager_closes_on_success_and_caller_failure(self) -> None:
        async with self.client as entered:
            self.assertIs(entered, self.client)
        self.session.close.assert_awaited_once_with()
        self.session.close.reset_mock()
        with self.assertRaisesRegex(ValueError, "caller failed"):
            async with self.client:
                raise ValueError("caller failed")
        self.session.close.assert_awaited_once_with()
