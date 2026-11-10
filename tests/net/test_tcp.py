import asyncio
import socket
import struct
import unittest
from typing import cast
from unittest.mock import patch

from tests.helpers import ManualClock
from tests.net.helpers import close_writer, read_message
from velis.config import (
    DEFAULT_SESSION_LIMITS,
    ServerLimits,
    SessionLimits,
    StoreLimits,
)
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.messages import (
    MISSING,
    NO_EXPIRY,
    Err,
    Lifetime,
    Message,
    Nil,
    Ok,
    Read,
    Send,
    Sent,
    Value,
    Watch,
    Watching,
    Write,
)
from velis.core.store import MAX_TTL_MS, Store
from velis.errors import CommandError, ProtocolError
from velis.net import protocol
from velis.net.client import Client
from velis.net.connection import COMMANDS_PER_TURN, Connection
from velis.net.server import TCPServer


class TCPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.clock = ManualClock()
        self.store = Store(self.clock, limits=StoreLimits(max_bytes=4096))
        self.broker = Broker(max_subscriptions=2)
        self.engine = Engine(self.store, self.broker)
        self.server = TCPServer(self.engine, ServerLimits(port=0))
        await self.server.start()
        self.addAsyncCleanup(self.server.close)

    async def connect_raw(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        self.addAsyncCleanup(close_writer, writer)
        return reader, writer

    async def connect_client(
        self, limits: SessionLimits = DEFAULT_SESSION_LIMITS
    ) -> Client:
        client = await Client.connect("127.0.0.1", self.server.port, limits=limits)
        self.addAsyncCleanup(client.close)
        return client

    async def test_typed_client_binary_values_ttls_and_persistence(self) -> None:
        client = await self.connect_client()
        async with asyncio.timeout(5):
            self.assertIsNone(await client.read("key"))
            self.assertEqual(await client.remaining("key"), MISSING)
            await client.write("key", bytes(range(256)))
            self.assertEqual(await client.read("key"), bytes(range(256)))
            self.assertEqual(await client.remaining("key"), NO_EXPIRY)
            self.assertTrue(await client.lifetime("key", 10))
            self.clock.advance(3)
            self.assertEqual(await client.remaining("key"), 7)
            self.assertTrue(await client.keep("key"))
            self.clock.advance(20)
            self.assertEqual(await client.read("key"), bytes(range(256)))
            self.assertTrue(await client.remove("key"))
            self.assertFalse(await client.remove("key"))

    async def test_concurrent_requests_and_self_publication_keep_replies_ordered(
        self,
    ) -> None:
        client = await self.connect_client(SessionLimits(max_pending_requests=3))
        for i in range(16):
            self.store.set(str(i), str(i).encode())
        async with asyncio.timeout(5):
            await client.watch("news")
            replies = await asyncio.gather(
                *(client.read(str(i % 16)) for i in range(100)),
                client.send("news", b"push"),
            )
            self.assertEqual(replies[:-1], [str(i % 16).encode() for i in range(100)])
            self.assertEqual(replies[-1], 1)
            self.assertEqual(await client.next_message(), Message("news", b"push"))

    async def test_command_and_local_encoding_errors_leave_connection_usable(
        self,
    ) -> None:
        client = await self.connect_client()
        async with asyncio.timeout(5):
            await client.write("k", b"original")
            with self.assertRaisesRegex(CommandError, "ttl out of range"):
                await client.lifetime("k", MAX_TTL_MS + 1)
            with self.assertRaisesRegex(CommandError, "byte limit"):
                await client.write("k", b"x" * 4096)
            with self.assertRaises(ProtocolError):
                await client.read("")
            self.assertEqual(await client.read("k"), b"original")

    async def test_pubsub_duplicate_subscriptions_limit_and_unsubscribe(self) -> None:
        subscriber, publisher = await self.connect_client(), await self.connect_client()
        async with asyncio.timeout(5):
            await subscriber.watch("a")
            await subscriber.watch("a")
            await subscriber.watch("b")
            with self.assertRaisesRegex(CommandError, "subscription limit"):
                await subscriber.watch("c")
            self.assertEqual(await publisher.send("a", b"\x00\xff"), 1)
            self.assertEqual(await subscriber.next_message(), Message("a", b"\x00\xff"))
            await subscriber.unwatch("a")
            self.assertEqual(await publisher.send("a", b"ignored"), 0)
            await subscriber.watch("c")

    async def test_fragmented_frames_and_pipelined_burst(self) -> None:
        reader, writer = await self.connect_raw()
        for byte in protocol.encode_command(Write("k", b"v")):
            writer.write(bytes([byte]))
            await writer.drain()
            await asyncio.sleep(0)
        self.assertEqual(await read_message(reader), Ok())
        total = COMMANDS_PER_TURN * 3 + 1
        writer.write(protocol.encode_command(Read("k")) * total)
        self.assertEqual(
            [await read_message(reader) for _ in range(total)], [Value(b"v")] * total
        )

    async def test_pipeline_recovers_after_command_error(self) -> None:
        reader, writer = await self.connect_raw()
        writer.write(
            b"".join(
                map(
                    protocol.encode_command,
                    (Write("k", b"v"), Lifetime("k", MAX_TTL_MS + 1), Read("k")),
                )
            )
        )
        self.assertEqual(
            [await read_message(reader) for _ in range(3)],
            [Ok(), Err("ttl out of range"), Value(b"v")],
        )

    async def test_client_batch_orders_dependent_commands_and_error_replies(
        self,
    ) -> None:
        client = await self.connect_client(SessionLimits(max_pending_requests=3))
        commands = [
            Write("k", b"original"),
            Read("k"),
            Lifetime("k", MAX_TTL_MS + 1),
            Write("k", b"new"),
            Read("k"),
        ]
        async with asyncio.timeout(5):
            self.assertEqual(
                await client.batch(commands),
                [
                    Ok(),
                    Value(b"original"),
                    Err("ttl out of range"),
                    Ok(),
                    Value(b"new"),
                ],
            )
            self.assertEqual(await client.read("k"), b"new")

    async def test_client_batch_separates_self_publications_from_replies(self) -> None:
        client = await self.connect_client(SessionLimits(max_pending_requests=2))
        async with asyncio.timeout(5):
            replies = await client.batch(
                [Watch("news"), Send("news", b"push"), Read("missing")]
            )
            self.assertEqual(replies, [Watching("news"), Sent(1), Nil()])
            self.assertEqual(await client.next_message(), Message("news", b"push"))

    async def test_client_batch_spans_server_turns_and_remains_usable(self) -> None:
        client = await self.connect_client(
            SessionLimits(max_pending_requests=COMMANDS_PER_TURN + 3)
        )
        total = COMMANDS_PER_TURN * 2 + 5
        commands = [Write(str(i), str(i).encode()) for i in range(total)]
        async with asyncio.timeout(5):
            self.assertEqual(await client.batch(commands), [Ok()] * total)
            self.assertEqual(
                await client.batch(Read(str(i)) for i in range(total)),
                [Value(str(i).encode()) for i in range(total)],
            )
            self.assertEqual(await client.read("0"), b"0")

    async def test_invalid_client_batch_sends_no_mutations(self) -> None:
        client = await self.connect_client()
        async with asyncio.timeout(5):
            with self.assertRaises(ProtocolError):
                await client.batch([Write("k", b"must not be written"), Read("")])
            self.assertIsNone(await client.read("k"))

    async def test_protocol_error_flushes_prior_replies_and_stops_later_mutations(
        self,
    ) -> None:
        reader, writer = await self.connect_raw()
        bad = protocol.encode_frame(
            b"\x00\x00\x00\x03BAD", protocol.MAX_COMMAND_BODY_BYTES
        )
        with self.assertLogs("velis.net.connection", "WARNING"):
            writer.write(
                protocol.encode_command(Write("before", b"v"))
                + bad
                + protocol.encode_command(Write("after", b"v"))
            )
            self.assertEqual(await read_message(reader), Ok())
            self.assertIsInstance(await read_message(reader), Err)
            async with asyncio.timeout(5):
                self.assertEqual(await reader.read(), b"")
        self.assertEqual(self.store.get("before"), b"v")
        self.assertIsNone(self.store.get("after"))

    async def test_oversized_header_is_rejected_without_receiving_the_body(
        self,
    ) -> None:
        reader, writer = await self.connect_raw()
        with self.assertLogs("velis.net.connection", "WARNING"):
            writer.write(struct.pack(">I", protocol.MAX_COMMAND_BODY_BYTES + 1))
            self.assertIsInstance(await read_message(reader), Err)
            async with asyncio.timeout(5):
                self.assertEqual(await reader.read(), b"")

    async def test_half_close_drains_complete_requests_and_discards_partial_tail(
        self,
    ) -> None:
        reader, writer = await self.connect_raw()
        total = COMMANDS_PER_TURN * 2 + 1
        writer.write(
            protocol.encode_command(Read("missing")) * total
            + protocol.encode_command(Write("tail", b"v"))[:-1]
        )
        writer.write_eof()
        self.assertEqual(
            [await read_message(reader) for _ in range(total)], [Nil()] * total
        )
        async with asyncio.timeout(5):
            self.assertEqual(await reader.read(), b"")
        self.assertIsNone(self.store.get("tail"))

    async def test_disconnect_removes_all_subscriptions(self) -> None:
        client = await self.connect_client()
        await client.watch("a")
        await client.watch("b")
        disconnected = asyncio.Event()
        original = self.engine.disconnect

        def disconnect(subscriber) -> None:
            original(subscriber)
            disconnected.set()

        with patch.object(self.engine, "disconnect", side_effect=disconnect):
            await client.close()
            async with asyncio.timeout(5):
                await disconnected.wait()
        self.assertEqual(self.broker.subscribers("a"), 0)
        self.assertEqual(self.broker.subscribers("b"), 0)

    async def test_server_close_closes_clients_and_can_be_repeated(self) -> None:
        client = await self.connect_client()
        await client.watch("a")
        await self.server.close()
        await self.server.close()
        self.assertEqual(self.broker.subscribers("a"), 0)
        with self.assertRaisesRegex(RuntimeError, "not started"):
            _ = self.server.port
        async with asyncio.timeout(5):
            with self.assertRaises(ConnectionError):
                await client.next_message()

    async def test_server_shutdown_aborts_a_subscriber_that_stops_reading(self) -> None:
        """Small kernel buffers force backpressure with a modest payload."""
        accepted: list[asyncio.Transport] = []
        original = Connection.connection_made

        def connection_made(
            connection: Connection, transport: asyncio.BaseTransport
        ) -> None:
            transport.get_extra_info("socket").setsockopt(
                socket.SOL_SOCKET, socket.SO_SNDBUF, 4096
            )
            original(connection, transport)
            accepted.append(cast(asyncio.Transport, transport))

        async with TCPServer(
            self.engine, ServerLimits(port=0, close_timeout_seconds=0.02)
        ) as server:
            raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.addCleanup(raw.close)
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            raw.setblocking(False)
            with patch.object(Connection, "connection_made", connection_made):
                await asyncio.get_running_loop().sock_connect(
                    raw, ("127.0.0.1", server.port)
                )
                reader, writer = await asyncio.open_connection(sock=raw)
                self.addAsyncCleanup(close_writer, writer)
                writer.write(protocol.encode_command(Watch("slow")))
                self.assertEqual(await read_message(reader), Watching("slow"))
            cast(asyncio.Transport, writer.transport).pause_reading()
            self.assertEqual(self.broker.publish("slow", b"x" * (1 << 20)), 1)
            self.assertGreater(accepted[0].get_write_buffer_size(), 0)
            async with asyncio.timeout(1):
                await server.close()
            self.assertEqual(self.broker.subscribers("slow"), 0)

    async def test_starting_an_active_server_is_rejected(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "already started"):
            await self.server.start()

    async def test_connection_limit_recovers_after_an_admitted_client_disconnects(
        self,
    ) -> None:
        await self.server.close()
        server = TCPServer(self.engine, ServerLimits(port=0, max_connections=1))
        await server.start()
        self.addAsyncCleanup(server.close)
        first_reader, first_writer = await asyncio.open_connection(
            "127.0.0.1", server.port
        )
        self.addAsyncCleanup(close_writer, first_writer)
        first_writer.write(protocol.encode_command(Read("k")))
        self.assertEqual(await read_message(first_reader), Nil())
        rejected, rejected_writer = await asyncio.open_connection(
            "127.0.0.1", server.port
        )
        self.addAsyncCleanup(close_writer, rejected_writer)
        self.assertEqual(await read_message(rejected), Err("connection limit exceeded"))
        released = asyncio.Event()
        original = self.engine.disconnect

        def disconnect(subscriber) -> None:
            original(subscriber)
            released.set()

        with patch.object(self.engine, "disconnect", side_effect=disconnect):
            await close_writer(first_writer)
            async with asyncio.timeout(5):
                await released.wait()
        async with await Client.connect("127.0.0.1", server.port) as replacement:
            async with asyncio.timeout(5):
                self.assertIsNone(await replacement.read("k"))

    async def test_raw_pubsub_delivers_a_push_and_a_count_reply(self) -> None:
        subscriber, sub_writer = await self.connect_raw()
        publisher, pub_writer = await self.connect_raw()
        sub_writer.write(protocol.encode_command(Watch("news")))
        self.assertEqual(await read_message(subscriber), Watching("news"))
        pub_writer.write(protocol.encode_command(Send("news", b"hello")))
        self.assertEqual(await read_message(publisher), Sent(1))
        self.assertEqual(await read_message(subscriber), Message("news", b"hello"))
