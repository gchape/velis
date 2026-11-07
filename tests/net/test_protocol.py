import asyncio
import struct
import unittest

from velis.core.commands import (
    Delivered,
    Err,
    Expire,
    Get,
    Message,
    Nil,
    Ok,
    Publish,
    Set,
    Subscribe,
    Subscribed,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.errors import ProtocolError
from velis.net import protocol


def stream(data: bytes) -> asyncio.StreamReader:
    """Expose an encoded frame as a complete in-memory stream."""
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_command_roundtrips(self) -> None:
        for command in (
            Get("ქართული"),
            Set("key", bytes(range(256))),
            Expire("key", (1 << 64) - 1),
            Subscribe("news"),
            Unsubscribe("news"),
            Publish("news", b"\x00\xff"),
        ):
            with self.subTest(command=command):
                encoded = protocol.encode_command(command)
                self.assertEqual(await protocol.read_command(stream(encoded)), command)

    async def test_reply_and_message_roundtrips(self) -> None:
        for reply in (
            Ok(),
            Nil(),
            Value(b"\xff"),
            Err("bad command"),
            Subscribed("news"),
            Unsubscribed("news"),
            Delivered((1 << 32) - 1),
        ):
            with self.subTest(reply=reply):
                encoded = protocol.encode_reply(reply)
                self.assertEqual(await protocol.read_frame(stream(encoded)), reply)
        payload = b"x" * protocol.MAX_LEN
        self.assertEqual(
            await protocol.read_frame(stream(protocol.encode_message("news", payload))),
            Message("news", payload),
        )

    async def test_encoders_reject_oversized_fields(self) -> None:
        big = b"x" * (protocol.MAX_LEN + 1)
        encoders = (
            lambda: protocol.encode_command(Set("key", big)),
            lambda: protocol.encode_command(Get("é" * (protocol.MAX_LEN // 2 + 1))),
            lambda: protocol.encode_reply(Value(big)),
            lambda: protocol.encode_message("news", big),
            lambda: protocol.encode_message("x" * (protocol.MAX_LEN + 1), b""),
        )
        for encode in encoders:
            with self.assertRaisesRegex(ProtocolError, "field too long"):
                encode()

    async def test_encoders_reject_invalid_integer_ranges(self) -> None:
        for value in (-1, 1 << 64):
            with self.subTest(ttl=value), self.assertRaises(ProtocolError):
                protocol.encode_command(Expire("key", value))
        for value in (-1, 1 << 32):
            with self.subTest(delivered=value), self.assertRaises(ProtocolError):
                protocol.encode_reply(Delivered(value))

    async def test_encoder_rejects_invalid_utf8(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "invalid utf-8"):
            protocol.encode_command(Get("\ud800"))

    async def test_decoder_rejects_invalid_fields(self) -> None:
        frames = (
            struct.pack(">I", protocol.MAX_LEN + 1),
            b"\x00\x00\x00\x01\xff",
            b"\x00\x00\x00\x04NOPE",
        )
        for frame in frames:
            with self.subTest(frame=frame), self.assertRaises(ProtocolError):
                await protocol.read_command(stream(frame))

    async def test_truncated_command_raises_incomplete_read(self) -> None:
        encoded = protocol.encode_command(Set("key", b"value"))
        with self.assertRaises(asyncio.IncompleteReadError):
            await protocol.read_command(stream(encoded[:-1]))
