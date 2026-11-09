import asyncio
import struct
import unittest
from collections.abc import Callable

from tests.net.test_fast import COMMAND_CASES, MESSAGE_CASES
from velis.core.messages import Err, Message, Publish, Set, Value
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.limits import (
    MAX_CHANNEL_CHARS,
    MAX_FIELD_BYTES,
    MAX_FRAME_BODY_BYTES,
    MAX_KEY_CHARS,
)


def stream(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    def test_framed_bytes_match_the_golden_bodies(self) -> None:
        def check[T](
            encode: Callable[[T], bytes], cases: tuple[tuple[T, bytes], ...]
        ) -> None:
            for value, body in cases:
                expected = struct.pack(">I", len(body)) + body
                with self.subTest(value=value):
                    self.assertEqual(encode(value), expected)
                    self.assertEqual(
                        list(protocol.iter_frames(bytearray(expected))),
                        [(body, len(expected))],
                    )

        check(protocol.encode_command, COMMAND_CASES)
        check(protocol.encode_message, MESSAGE_CASES)

    def test_payload_and_outer_frame_limits(self) -> None:
        data = b"x" * MAX_FIELD_BYTES
        for value in (
            Set("\U0010ffff" * MAX_KEY_CHARS, data),
            Publish("\U0010ffff" * MAX_CHANNEL_CHARS, data),
        ):
            encoded = protocol.encode_command(value)
            [(body, end)] = protocol.iter_frames(bytearray(encoded))
            self.assertEqual(end, len(encoded))
            self.assertEqual(protocol.COMMANDS.decode(body), value)
        for value in (Set("k", data + b"x"), Publish("c", data + b"x")):
            with self.assertRaisesRegex(ProtocolError, "field too long"):
                protocol.encode_command(value)
        for value in (
            Value(data + b"x"),
            Message("c", data + b"x"),
            Err("x" * (MAX_FIELD_BYTES + 1)),
        ):
            with self.assertRaisesRegex(ProtocolError, "field too long"):
                protocol.encode_message(value)
        self.assertEqual(
            len(protocol.encode_frame(b"x" * MAX_FRAME_BODY_BYTES)),
            MAX_FRAME_BODY_BYTES + 4,
        )
        with self.assertRaisesRegex(ProtocolError, "field too long"):
            protocol.encode_frame(b"x" * (MAX_FRAME_BODY_BYTES + 1))
        with self.assertRaisesRegex(ProtocolError, "field too long"):
            protocol.encode_message(Message("x" * MAX_FIELD_BYTES, data))

    async def test_message_reader_roundtrips_all_server_frames(self) -> None:
        encoded = b"".join(protocol.encode_message(value) for value, _ in MESSAGE_CASES)
        self.assertEqual(
            [value async for value in protocol.read_messages(stream(encoded))],
            [value for value, _ in MESSAGE_CASES],
        )
        self.assertEqual(
            [value async for value in protocol.read_messages(stream(b""))], []
        )
        for partial in (encoded[:1], encoded[:3], encoded[:-1]):
            with (
                self.subTest(partial=partial),
                self.assertRaises(asyncio.IncompleteReadError),
            ):
                [value async for value in protocol.read_messages(stream(partial))]
