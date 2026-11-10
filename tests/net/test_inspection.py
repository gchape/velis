"""Check inspection commands across codecs, typed clients and real TCP."""

import struct
import unittest
from typing import cast
from unittest.mock import Mock

from velis.app import build_engine
from velis.core.messages import (
    Exists,
    Found,
    KeyPage,
    ListKeys,
    Nil,
    Ping,
    Pong,
    Size,
    SizeValue,
)
from velis.errors import CommandError, ProtocolError
from velis.net.client import Client
from velis.net.protocol import COMMANDS, SERVER_MESSAGES
from velis.net.server import ServerLimits, TCPServer
from velis.net.session import Session
from velis.net.wire import text_tuple


class InspectionWireTests(unittest.TestCase):
    def test_prefix_and_page_keys_preserve_unicode_and_control_characters(self) -> None:
        prefix = "界\x1b"
        command = ListKeys(prefix, 3, 10)
        reply = KeyPage(4, (prefix, "", "a\n\tb"))
        self.assertEqual(COMMANDS.decode(COMMANDS.encode(command)), command)
        self.assertEqual(SERVER_MESSAGES.decode(SERVER_MESSAGES.encode(reply)), reply)

    def test_prefix_and_tuple_limits_are_checked_before_allocation(self) -> None:
        with self.assertRaises(ProtocolError):
            COMMANDS.encode(ListKeys("x" * 2049))
        codec = text_tuple(2, 4)
        for values in (
            ("a", "b", "c"),
            ("xxxxx",),
            cast(tuple[str, ...], (1,)),
            cast(tuple[str, ...], ["a"]),
        ):
            with self.subTest(values=values), self.assertRaises(ProtocolError):
                codec.write(values)
        with self.assertRaisesRegex(ProtocolError, "too many text items"):
            codec.read(struct.pack(">I", 2**32 - 1), 0, 4)

    def test_every_truncated_key_page_is_rejected(self) -> None:
        body = SERVER_MESSAGES.encode(KeyPage(19, ("first", "界", "last")))
        for end in range(len(body)):
            with self.subTest(end=end), self.assertRaises(ProtocolError):
                SERVER_MESSAGES.decode(body[:end])


class InspectionClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_typed_commands_validate_their_replies(self) -> None:
        session = Mock(spec=Session)
        client = Client(session)
        for name, args, command, reply, expected in (
            ("ping", (), Ping(), Pong(), None),
            ("exists", ("key",), Exists("key"), Found(), True),
            ("exists", ("key",), Exists("key"), Nil(), False),
            ("size", ("key",), Size("key"), SizeValue(0), 0),
            ("size", ("key",), Size("key"), Nil(), None),
            ("list_keys", (), ListKeys(), KeyPage(0, ()), KeyPage(0, ())),
        ):
            with self.subTest(name=name, reply=reply):
                session.request.return_value = reply
                self.assertEqual(await getattr(client, name)(*args), expected)
                session.request.assert_awaited_with(command)

    async def test_server_inspection_commands_work_over_tcp(self) -> None:
        async with (
            TCPServer(build_engine(), ServerLimits(port=0)) as server,
            await Client.connect(port=server.port) as client,
        ):
            await client.ping()
            await client.write("prefix:界\x1b", b"\x00\xff")
            await client.write("other", b"")
            self.assertTrue(await client.exists("other"))
            self.assertEqual(await client.size("other"), 0)
            self.assertEqual(await client.size("prefix:界\x1b"), 2)
            self.assertEqual(
                await client.list_keys("prefix:"), KeyPage(0, ("prefix:界\x1b",))
            )
            await client.lifetime("other", 0)
            self.assertFalse(await client.exists("other"))
            self.assertIsNone(await client.size("other"))
            with self.assertRaises(CommandError):
                await client.list_keys(limit=101)
            await client.ping()
