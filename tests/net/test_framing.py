import asyncio
import struct
import unittest
from unittest.mock import AsyncMock, Mock

from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.limits import MAX_FRAME_BODY_BYTES


async def feed_chunks(
    reader: asyncio.StreamReader, data: bytes, chunk_size: int
) -> None:
    for offset in range(0, len(data), chunk_size):
        reader.feed_data(data[offset : offset + chunk_size])
        await asyncio.sleep(0)
    reader.feed_eof()


class StreamFramingTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragmented_and_pipelined_frames(self) -> None:
        bodies = [b"one", b"", b"three"]
        encoded = b"".join(protocol.encode_frame(body) for body in bodies)
        for chunk_size in range(1, len(encoded) + 1):
            with self.subTest(chunk_size=chunk_size):
                reader = asyncio.StreamReader()
                async with asyncio.TaskGroup() as tasks:
                    tasks.create_task(feed_chunks(reader, encoded, chunk_size))
                    for body in bodies:
                        self.assertEqual(await protocol._read_body(reader), body)
                    self.assertIsNone(await protocol._read_body(reader))

    async def test_oversized_header_fails_before_body_is_requested(self) -> None:
        reader = Mock(spec=asyncio.StreamReader)
        reader.readexactly = AsyncMock(
            return_value=struct.pack(">I", MAX_FRAME_BODY_BYTES + 1)
        )
        with self.assertRaisesRegex(ProtocolError, "field too long"):
            await protocol._read_body(reader)
        reader.readexactly.assert_awaited_once_with(4)
        with self.assertRaisesRegex(ProtocolError, "field too long"):
            list(
                protocol.iter_frames(
                    bytearray(struct.pack(">I", MAX_FRAME_BODY_BYTES + 1))
                )
            )

    async def test_clean_eof_and_truncated_next_frame(self) -> None:
        first = protocol.encode_frame(b"first")
        second = protocol.encode_frame(b"abcdef")
        for values in ([], [b"first"], [b"first", b"abcdef"]):
            reader = asyncio.StreamReader()
            reader.feed_data(b"".join(protocol.encode_frame(body) for body in values))
            reader.feed_eof()
            for body in values:
                self.assertEqual(await protocol._read_body(reader), body)
            self.assertIsNone(await protocol._read_body(reader))
        for end in range(1, len(second)):
            with self.subTest(end=end):
                reader = asyncio.StreamReader()
                reader.feed_data(first + second[:end])
                reader.feed_eof()
                self.assertEqual(await protocol._read_body(reader), b"first")
                with self.assertRaises(asyncio.IncompleteReadError) as raised:
                    await protocol._read_body(reader)
                if end < 4:
                    self.assertEqual(raised.exception.expected, 4)
                    self.assertEqual(raised.exception.partial, second[:end])
                else:
                    self.assertEqual(raised.exception.expected, len(b"abcdef"))
                    self.assertEqual(raised.exception.partial, second[4:end])
                buf = bytearray(first + second[:end])
                self.assertEqual(
                    list(protocol.iter_frames(buf)), [(b"first", len(first))]
                )
