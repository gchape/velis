import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from tests.codec.helpers import Reverse
from velis.codec import Identity, LengthPrefixedField, UInt
from velis.errors import ProtocolError
from velis.net.protocol import WireProtocol


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
        for prefix_length in (1, 2, 3, 4, 8):
            for codec in (Identity(), Reverse()):
                framing = LengthPrefixedField(codec, 100, prefix=UInt(prefix_length))
                wire = WireProtocol(framing=framing)
                encoded = b"".join(framing.encode(body) for body in bodies)
                for chunk_size in range(1, len(encoded) + 1):
                    with self.subTest(
                        prefix_length=prefix_length, codec=codec, chunk_size=chunk_size
                    ):
                        reader = asyncio.StreamReader()
                        async with asyncio.TaskGroup() as tasks:
                            tasks.create_task(feed_chunks(reader, encoded, chunk_size))
                            decoded = [body async for body in wire._read_bodies(reader)]
                        self.assertEqual(decoded, bodies)

    async def test_oversized_header_fails_before_body_is_requested(self) -> None:
        for prefix_length in (1, 2, 3, 4, 8):
            with self.subTest(prefix_length=prefix_length):
                framing = LengthPrefixedField(
                    Identity(), 10, prefix=UInt(prefix_length)
                )
                wire = WireProtocol(framing=framing)
                reader = Mock(spec=asyncio.StreamReader)
                reader.readexactly = AsyncMock(
                    return_value=(11).to_bytes(prefix_length)
                )
                with self.assertRaisesRegex(ProtocolError, "too long"):
                    await anext(wire._read_bodies(reader))
                reader.readexactly.assert_awaited_once_with(prefix_length)

    async def test_clean_eof_and_truncated_next_frame(self) -> None:
        for prefix_length in (1, 2, 3, 4, 8):
            framing = LengthPrefixedField(Identity(), 100, prefix=UInt(prefix_length))
            wire = WireProtocol(framing=framing)
            first = framing.encode(b"first")
            second = framing.encode(b"abcdef")
            for complete in (b"", first, first + second):
                with self.subTest(prefix_length=prefix_length, complete=complete):
                    reader = asyncio.StreamReader()
                    reader.feed_data(complete)
                    reader.feed_eof()
                    expected = [] if not complete else [b"first"]
                    if complete == first + second:
                        expected.append(b"abcdef")
                    self.assertEqual(
                        [body async for body in wire._read_bodies(reader)], expected
                    )
            for end in range(1, len(second)):
                with self.subTest(prefix_length=prefix_length, end=end):
                    reader = asyncio.StreamReader()
                    reader.feed_data(first + second[:end])
                    reader.feed_eof()
                    bodies = wire._read_bodies(reader)
                    self.assertEqual(await anext(bodies), b"first")
                    with self.assertRaises(asyncio.IncompleteReadError) as raised:
                        await anext(bodies)
                    if end < prefix_length:
                        self.assertEqual(raised.exception.expected, prefix_length)
                        self.assertEqual(raised.exception.partial, second[:end])
                    else:
                        self.assertEqual(raised.exception.expected, len(b"abcdef"))
                        self.assertEqual(
                            raised.exception.partial, second[prefix_length:end]
                        )
