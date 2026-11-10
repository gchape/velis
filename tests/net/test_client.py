import unittest
from unittest.mock import AsyncMock, Mock, patch

from velis.config import SessionLimits
from velis.core.messages import (
    MISSING,
    NO_EXPIRY,
    Err,
    Keep,
    Lifetime,
    Message,
    Nil,
    Ok,
    Read,
    Remaining,
    RemainingValue,
    Remove,
    Send,
    Sent,
    Unlimited,
    Unwatch,
    Unwatched,
    Value,
    Watch,
    Watching,
    Write,
)
from velis.errors import CommandError, ProtocolError
from velis.net.client import Client, EntrySnapshot
from velis.net.session import Session


class ClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = Mock(spec=Session)
        self.session.request = AsyncMock()
        self.session.next_message = AsyncMock()
        self.session.close = AsyncMock()
        self.client = Client(self.session)

    async def test_read_entry_validates_value_and_lifetime(self) -> None:
        for value, remaining, expected in (
            (Value(b""), Unlimited(), EntrySnapshot(b"", NO_EXPIRY)),
            (Value(b"data"), RemainingValue(25), EntrySnapshot(b"data", 25)),
            (Nil(), Nil(), EntrySnapshot(None, MISSING)),
        ):
            with self.subTest(expected=expected):
                self.session.batch.return_value = [value, remaining]
                self.assertEqual(await self.client.read_entry("k"), expected)
                self.session.batch.assert_awaited_with((Read("k"), Remaining("k")))

    async def test_read_entry_rejects_invalid_lifetime_even_when_value_missing(
        self,
    ) -> None:
        for replies, error in (
            ([Nil(), Err("refused")], CommandError),
            ([Nil(), Ok()], ProtocolError),
            ([Nil()], ProtocolError),
        ):
            with self.subTest(replies=replies):
                self.session.batch.return_value = replies
                with self.assertRaises(error):
                    await self.client.read_entry("k")

    async def test_bulk_lifetimes_preserve_errors_and_duplicate_key_positions(
        self,
    ) -> None:
        self.session.batch.return_value = [Nil(), Err("refused"), RemainingValue(9)]
        result = await self.client.remaining_many(iter(("a", "b", "a")))
        self.assertEqual(result[0], MISSING)
        self.assertIsInstance(result[1], CommandError)
        self.assertEqual(str(result[1]), "refused")
        self.assertEqual(result[2], 9)
        self.session.batch.assert_awaited_once_with(
            (Remaining("a"), Remaining("b"), Remaining("a"))
        )

    async def test_bulk_lifetimes_reject_malformed_replies_and_count_mismatch(
        self,
    ) -> None:
        for replies in ([Ok()], [], [Nil(), Nil()]):
            with self.subTest(replies=replies):
                self.session.batch.return_value = replies
                with self.assertRaises(ProtocolError):
                    await self.client.remaining_many(["a"])
        self.session.batch.return_value = []
        self.assertEqual(await self.client.remaining_many([]), [])

    async def test_commands_and_reply_values(self) -> None:
        cases = (
            ("read", ("k",), Read("k"), Value(b"\x00\xff"), b"\x00\xff"),
            ("read", ("k",), Read("k"), Nil(), None),
            ("write", ("k", b"v"), Write("k", b"v"), Ok(), None),
            ("lifetime", ("k", 12), Lifetime("k", 12), Ok(), True),
            ("lifetime", ("k", 12), Lifetime("k", 12), Nil(), False),
            ("remove", ("k",), Remove("k"), Ok(), True),
            ("remove", ("k",), Remove("k"), Nil(), False),
            ("keep", ("k",), Keep("k"), Ok(), True),
            ("keep", ("k",), Keep("k"), Nil(), False),
            ("remaining", ("k",), Remaining("k"), Nil(), MISSING),
            ("remaining", ("k",), Remaining("k"), Unlimited(), NO_EXPIRY),
            ("remaining", ("k",), Remaining("k"), RemainingValue(12), 12),
            ("watch", ("c",), Watch("c"), Watching("c"), None),
            ("unwatch", ("c",), Unwatch("c"), Unwatched("c"), None),
            ("send", ("c", b"v"), Send("c", b"v"), Sent(3), 3),
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
            ("read", ("k",)),
            ("write", ("k", b"v")),
            ("lifetime", ("k", 1)),
            ("remove", ("k",)),
            ("keep", ("k",)),
            ("remaining", ("k",)),
            ("watch", ("c",)),
            ("unwatch", ("c",)),
            ("send", ("c", b"v")),
        ):
            with self.subTest(method=method):
                self.session.request.return_value = Err("rejected")
                with self.assertRaisesRegex(CommandError, "rejected"):
                    await getattr(self.client, method)(*args)

    async def test_unexpected_reply_types_and_wrong_echoed_channels_are_rejected(
        self,
    ) -> None:
        for method, args, reply in (
            ("read", ("k",), Ok()),
            ("write", ("k", b"v"), Nil()),
            ("lifetime", ("k", 1), Value(b"v")),
            ("remove", ("k",), Unlimited()),
            ("keep", ("k",), RemainingValue(1)),
            ("remaining", ("k",), Ok()),
            ("watch", ("c",), Watching("wrong")),
            ("unwatch", ("c",), Unwatched("wrong")),
            ("send", ("c", b"v"), Ok()),
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
        commands = [Write("k", b"v"), Read("k"), Lifetime("k", 1)]
        replies = [Ok(), Value(b"v"), Err("rejected")]
        self.session.batch.return_value = replies
        self.assertIs(await self.client.batch(commands), replies)
        self.session.batch.assert_awaited_once_with(commands)
        self.session.request.assert_not_awaited()

    async def test_batch_propagates_transport_failures(self) -> None:
        self.session.batch.side_effect = ConnectionError("batch failed")
        with self.assertRaisesRegex(ConnectionError, "batch failed"):
            await self.client.batch([Read("k")])

    async def test_context_manager_closes_on_success_and_caller_failure(self) -> None:
        async with self.client as entered:
            self.assertIs(entered, self.client)
        self.session.close.assert_awaited_once_with()
        self.session.close.reset_mock()
        with self.assertRaisesRegex(ValueError, "caller failed"):
            async with self.client:
                raise ValueError("caller failed")
        self.session.close.assert_awaited_once_with()
