import asyncio
import struct
import unittest
from unittest.mock import Mock

from velis.core.messages import Message, Read, Write
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.limits import MAX_CHANNEL_BYTES, MAX_FIELD_BYTES, MAX_KEY_CHARS


class FramingTests(unittest.TestCase):
    def test_all_partial_headers_and_bodies_remain_buffered(self) -> None:
        frame = protocol.encode_command(Read("key"))
        for end in range(len(frame)):
            with self.subTest(end=end):
                buf = bytearray(frame[:end])
                self.assertEqual(list(protocol.iter_frames(buf)), [])
                self.assertEqual(buf, frame[:end])

    def test_multiple_frames_report_end_offsets_without_consuming_buffer(self) -> None:
        first = protocol.encode_command(Read("a"))
        second = protocol.encode_command(Read("bb"))
        buf = bytearray(first + second + b"\x00\x00")
        self.assertEqual(
            list(protocol.iter_frames(buf)),
            [(first[4:], len(first)), (second[4:], len(first) + len(second))],
        )
        self.assertEqual(buf, first + second + b"\x00\x00")

    def test_exact_limit_and_empty_frame_are_allowed(self) -> None:
        self.assertEqual(protocol.encode_frame(b"abc", 3), b"\x00\x00\x00\x03abc")
        self.assertEqual(
            list(protocol.iter_frames(bytearray(b"\x00" * 4), 0)), [(b"", 4)]
        )
        with self.assertRaises(ProtocolError):
            protocol.encode_frame(b"abcd", 3)

    def test_oversized_header_fails_after_yielding_previous_valid_frames(self) -> None:
        frame = protocol.encode_command(Read("a"))
        iterator = protocol.iter_frames(
            bytearray(frame + struct.pack(">I", protocol.MAX_COMMAND_BODY_BYTES + 1))
        )
        self.assertEqual(next(iterator), (frame[4:], len(frame)))
        with self.assertRaisesRegex(ProtocolError, "field too long"):
            next(iterator)

    def test_directional_limits_equal_the_largest_encodable_body(self) -> None:
        command = Write("😀" * MAX_KEY_CHARS, b"x" * MAX_FIELD_BYTES)
        push = Message("a" * MAX_CHANNEL_BYTES, b"x" * MAX_FIELD_BYTES)
        self.assertEqual(
            len(protocol.encode_command(command)) - 4, protocol.MAX_COMMAND_BODY_BYTES
        )
        self.assertEqual(
            len(protocol.encode_message(push)) - 4, protocol.MAX_SERVER_BODY_BYTES
        )
        self.assertGreater(
            protocol.MAX_COMMAND_BODY_BYTES, protocol.MAX_SERVER_BODY_BYTES
        )

    def test_push_cache_uses_identity_and_keeps_only_the_latest_message(self) -> None:
        encode = Mock(side_effect=protocol.encode_message)
        cache = protocol.PushEncoder(encode)
        first = Message("a", b"v")
        equal = Message("a", b"v")
        encoded = cache(first)
        self.assertIs(cache(first), encoded)
        self.assertEqual(encode.call_count, 1)
        self.assertEqual(cache(equal), encoded)
        self.assertEqual(encode.call_count, 2)
        self.assertEqual(cache(first), encoded)
        self.assertEqual(encode.call_count, 3)

    def test_push_encoding_failure_preserves_previous_cached_result(self) -> None:
        encode = Mock(side_effect=[b"frame", ProtocolError("bad push")])
        cache = protocol.PushEncoder(encode)
        first = Message("a", b"v")
        self.assertEqual(cache(first), b"frame")
        with self.assertRaises(ProtocolError):
            cache(Message("b", b"v"))
        self.assertEqual(cache(first), b"frame")
        self.assertEqual(encode.call_count, 2)


class ReadFrameTests(unittest.IsolatedAsyncioTestCase):
    async def test_clean_eof_is_distinct_from_an_empty_body(self) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(b"\x00" * 4)
        reader.feed_eof()
        self.assertEqual(await protocol.read_frame(reader), b"")
        self.assertIsNone(await protocol.read_frame(reader))

    async def test_partial_headers_and_bodies_raise_incomplete_read(self) -> None:
        frame = protocol.encode_message(Message("a", b"v"))
        for end in range(1, len(frame)):
            with self.subTest(end=end):
                reader = asyncio.StreamReader()
                reader.feed_data(frame[:end])
                reader.feed_eof()
                with self.assertRaises(asyncio.IncompleteReadError):
                    await protocol.read_frame(reader)

    async def test_header_limit_is_checked_without_waiting_for_body_or_eof(
        self,
    ) -> None:
        for limit in (3, protocol.MAX_SERVER_BODY_BYTES):
            reader = asyncio.StreamReader()
            reader.feed_data(struct.pack(">I", limit + 1))
            async with asyncio.timeout(1):
                with self.assertRaisesRegex(ProtocolError, "field too long"):
                    await protocol.read_frame(reader, limit)

    async def test_default_read_limit_rejects_a_header_only_command_sized_frame(
        self,
    ) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack(">I", protocol.MAX_SERVER_BODY_BYTES + 1))
        async with asyncio.timeout(1):
            with self.assertRaisesRegex(ProtocolError, "field too long"):
                await protocol.read_frame(reader)

    async def test_fragmented_frame_waits_for_remaining_bytes(self) -> None:
        reader = asyncio.StreamReader()
        frame = protocol.encode_message(Message("a", b"v"))
        task = asyncio.create_task(protocol.read_frame(reader))
        try:
            for byte in frame[:-1]:
                reader.feed_data(bytes([byte]))
                await asyncio.sleep(0)
                self.assertFalse(task.done())
            reader.feed_data(frame[-1:])
            async with asyncio.timeout(1):
                self.assertEqual(await task, frame[4:])
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
