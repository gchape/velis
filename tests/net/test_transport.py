import asyncio
import contextlib
import struct
import unittest
from unittest.mock import patch

from velis.app import build_engine
from velis.config import ServerLimits
from velis.core.messages import (
    Command,
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
    Value,
)
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.connection import COMMANDS_PER_TURN, Connection
from velis.net.limits import MAX_FRAME_BODY_BYTES
from velis.net.protocol import ServerFrame
from velis.net.server import TCPServer


def frame(command: Command) -> bytes:
    return protocol.encode_command(command)


async def read_frame(reader: asyncio.StreamReader) -> ServerFrame:
    async with asyncio.timeout(10):
        (length,) = struct.unpack(">I", await reader.readexactly(4))
        return protocol.SERVER_MESSAGES.decode(await reader.readexactly(length))


async def close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()


class IterFramesTests(unittest.TestCase):
    def test_partial_header_and_partial_body_yield_nothing(self) -> None:
        data = frame(Get("k"))
        for partial in (data[:3], data[:-1]):
            buf = bytearray(partial)
            self.assertEqual(list(protocol.iter_frames(buf)), [])
            self.assertEqual(buf, partial)

    def test_two_frames_and_offsets_leave_the_buffer_unchanged(self) -> None:
        a, b = frame(Get("a")), frame(Get("bb"))
        buf = bytearray(a + b + b"\x00\x00")
        original = bytes(buf)
        got = list(protocol.iter_frames(buf))
        self.assertEqual([end for _, end in got], [len(a), len(a) + len(b)])
        self.assertEqual(buf, original)
        self.assertEqual(
            [protocol.COMMANDS.decode(body) for body, _ in got], [Get("a"), Get("bb")]
        )

    def test_oversized_header_rejected_before_body_arrives(self) -> None:
        with self.assertRaises(ProtocolError):
            list(
                protocol.iter_frames(
                    bytearray(struct.pack(">I", MAX_FRAME_BODY_BYTES + 1))
                )
            )

    def test_valid_frames_before_a_bad_header_are_still_yielded(self) -> None:
        good = frame(Get("k"))
        it = protocol.iter_frames(
            bytearray(good + struct.pack(">I", MAX_FRAME_BODY_BYTES + 1))
        )
        body, end = next(it)
        self.assertEqual(protocol.COMMANDS.decode(body), Get("k"))
        self.assertEqual(end, len(good))
        with self.assertRaises(ProtocolError):
            next(it)


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = TCPServer(build_engine(), ServerLimits(port=0))
        await self.server.start()
        self.addAsyncCleanup(self.server.close)

    async def connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        self.addAsyncCleanup(close_writer, writer)
        return reader, writer

    async def test_set_get_roundtrip(self) -> None:
        reader, writer = await self.connect()
        writer.write(frame(Set("k", b"v")) + frame(Get("k")) + frame(Get("missing")))
        self.assertEqual(
            [await read_frame(reader) for _ in range(3)], [Ok(), Value(b"v"), Nil()]
        )

    async def test_pipelined_burst_beyond_one_turn_keeps_order(self) -> None:
        reader, writer = await self.connect()
        n = 1000
        writer.write(
            b"".join(
                frame(Set(f"k{i}", str(i).encode())) + frame(Get(f"k{i}"))
                for i in range(n)
            )
        )
        for i in range(n):
            self.assertEqual(await read_frame(reader), Ok())
            self.assertEqual(await read_frame(reader), Value(str(i).encode()))

    async def test_frame_delivered_one_byte_at_a_time(self) -> None:
        reader, writer = await self.connect()
        for byte in frame(Set("k", b"v")) + frame(Get("k")):
            writer.write(bytes([byte]))
            await writer.drain()
            await asyncio.sleep(0)
        self.assertEqual(
            [await read_frame(reader), await read_frame(reader)], [Ok(), Value(b"v")]
        )

    async def test_command_error_keeps_connection_usable(self) -> None:
        reader, writer = await self.connect()
        writer.write(frame(Expire("k", 1 << 63)) + frame(Set("k", b"v")))
        self.assertIsInstance(await read_frame(reader), Err)
        self.assertEqual(await read_frame(reader), Ok())

    async def test_protocol_error_flushes_prior_replies_and_discards_later_commands(
        self,
    ) -> None:
        reader, writer = await self.connect()
        bad = protocol.encode_frame(struct.pack(">I", 3) + b"FOO")
        writer.write(frame(Set("k", b"v")) + bad + frame(Set("after", b"must not run")))
        self.assertEqual(await read_frame(reader), Ok())
        reply = await read_frame(reader)
        self.assertIsInstance(reply, Err)
        assert isinstance(reply, Err)
        self.assertIn("unknown command", reply.message)
        async with asyncio.timeout(1):
            self.assertEqual(await reader.read(), b"")
        other_reader, other_writer = await self.connect()
        other_writer.write(frame(Get("after")))
        self.assertEqual(await read_frame(other_reader), Nil())

    async def test_oversized_header_gets_err_then_close(self) -> None:
        reader, writer = await self.connect()
        writer.write(struct.pack(">I", MAX_FRAME_BODY_BYTES + 1))
        self.assertIsInstance(await read_frame(reader), Err)
        async with asyncio.timeout(1):
            self.assertEqual(await reader.read(), b"")

    async def test_pubsub_push(self) -> None:
        subscriber, sub_writer = await self.connect()
        publisher, pub_writer = await self.connect()
        sub_writer.write(frame(Subscribe("news")))
        self.assertEqual(await read_frame(subscriber), Subscribed("news"))
        pub_writer.write(frame(Publish("news", b"hello")))
        self.assertEqual(await read_frame(publisher), Delivered(1))
        self.assertEqual(await read_frame(subscriber), Message("news", b"hello"))

    async def test_half_close_drains_all_complete_requests_before_eof(self) -> None:
        reader, writer = await self.connect()
        total = COMMANDS_PER_TURN * 3 + 1
        writer.write(frame(Get("missing")) * total + frame(Get("tail"))[:-1])
        writer.write_eof()
        self.assertEqual(
            [await read_frame(reader) for _ in range(total)], [Nil()] * total
        )
        async with asyncio.timeout(1):
            self.assertEqual(await reader.read(), b"")

    async def test_slow_reader_backpressure_recovers_without_reordering(self) -> None:
        reader, writer = await self.connect()
        big = b"x" * (256 * 1024)
        writer.write(frame(Set("big", big)))
        self.assertEqual(await read_frame(reader), Ok())
        paused, resumed = asyncio.Event(), asyncio.Event()
        pause_writing, resume_writing = (
            Connection.pause_writing,
            Connection.resume_writing,
        )

        def pause(connection: Connection) -> None:
            pause_writing(connection)
            paused.set()

        def resume(connection: Connection) -> None:
            resume_writing(connection)
            resumed.set()

        with (
            patch.object(Connection, "pause_writing", pause),
            patch.object(Connection, "resume_writing", resume),
        ):
            writer.write(frame(Get("big")) * 100)
            async with asyncio.timeout(5):
                await paused.wait()
            for _ in range(100):
                self.assertEqual(await read_frame(reader), Value(big))
            self.assertTrue(resumed.is_set())
        writer.write(frame(Get("big")))
        self.assertEqual(await read_frame(reader), Value(big))


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_limit_then_release(self) -> None:
        engine = build_engine()
        server = TCPServer(engine, ServerLimits(port=0, max_connections=1))
        await server.start()
        self.addAsyncCleanup(server.close)
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        self.addAsyncCleanup(close_writer, writer)
        writer.write(frame(Set("k", b"v")))
        self.assertEqual(await read_frame(reader), Ok())
        rejected, rejected_writer = await asyncio.open_connection(
            "127.0.0.1", server.port
        )
        self.addAsyncCleanup(close_writer, rejected_writer)
        self.assertEqual(await read_frame(rejected), Err("connection limit exceeded"))
        released = asyncio.Event()
        original_disconnect = engine.disconnect

        def disconnect(connection: Connection) -> None:
            original_disconnect(connection)
            released.set()

        with patch.object(engine, "disconnect", side_effect=disconnect):
            async with asyncio.timeout(1):
                self.assertEqual(await rejected.read(), b"")
                await close_writer(writer)
                await released.wait()
        replacement, replacement_writer = await asyncio.open_connection(
            "127.0.0.1", server.port
        )
        self.addAsyncCleanup(close_writer, replacement_writer)
        replacement_writer.write(frame(Get("k")))
        self.assertEqual(await read_frame(replacement), Value(b"v"))
