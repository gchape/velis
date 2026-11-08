import asyncio
import struct
import unittest

from velis.codec import CodecChain, Deflate
from velis.config import Server as ServerConfig
from velis.core.broker import Broker
from velis.core.commands import (
    Err,
    Expire,
    Get,
    Message,
    Ok,
    Persistent,
    Set,
    Ttl,
    Value,
)
from velis.core.engine import Engine
from velis.core.store import MAX_TTL_MS, Store
from velis.net import protocol
from velis.net.client import Client
from velis.net.protocol import Request, Response
from velis.net.server import TcpServer


class RequestIdIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.store = Store()
        self.engine = Engine(self.store, Broker())
        self.server = TcpServer(self.engine, ServerConfig("127.0.0.1", 0))
        await self.server.start()
        self.addAsyncCleanup(self.server.close)

    async def test_raw_pipeline_echoes_each_id_including_command_errors(self) -> None:
        wire = protocol.DEFAULT_PROTOCOL
        requests = (
            Request(19, Set("key", b"value")),
            Request(3, Get("key")),
            Request(protocol.MAX_REQUEST_ID, Expire("key", MAX_TTL_MS + 1)),
            Request(42, Ttl("key")),
        )
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        try:
            writer.write(
                b"".join(wire.commands.encode(request) for request in requests)
            )
            async with asyncio.timeout(1):
                responses = [await wire.read_frame(reader) for _ in requests]
            self.assertEqual(
                responses,
                [
                    Response(19, Ok()),
                    Response(3, Value(b"value")),
                    Response(protocol.MAX_REQUEST_ID, Err("ttl out of range")),
                    Response(42, Persistent()),
                ],
            )
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_request_id_zero_gets_connection_error_then_eof(self) -> None:
        wire = protocol.DEFAULT_PROTOCOL
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        try:
            writer.write(
                wire.framing.encode(
                    struct.pack(">Q", 0)
                    + protocol.CommandCodec().encode(Set("key", b"value"))
                )
            )
            async with asyncio.timeout(1):
                self.assertEqual(
                    await wire.read_frame(reader),
                    Response(0, Err("request ID must be nonzero")),
                )
                self.assertEqual(await reader.read(), b"")
            self.assertIsNone(self.store.get("key"))
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_concurrent_commands_and_pushes_with_default_and_compressed_wires(
        self,
    ) -> None:
        compressed = protocol.WireProtocol(
            command_codec=CodecChain.of(protocol.CommandCodec())
            | Deflate(max_length=protocol.MAX_FRAME_LEN),
            frame_codec=CodecChain.of(protocol.FrameCodec())
            | Deflate(max_length=protocol.MAX_FRAME_LEN),
        )
        for i in range(16):
            self.store.set(str(i), str(i).encode())
        for wire in (protocol.DEFAULT_PROTOCOL, compressed):
            with self.subTest(wire=wire):
                server = TcpServer(self.engine, ServerConfig("127.0.0.1", 0), wire=wire)
                await server.start()
                try:
                    async with await Client.connect(
                        "127.0.0.1", server.port, wire=wire, max_pending_requests=3
                    ) as client:
                        await client.subscribe("news")
                        async with asyncio.timeout(5):
                            # More operations than slots exercise pipelining and
                            # capacity waiters while pushes share the same stream.
                            results = await asyncio.gather(
                                *(client.get(str(i % 16)) for i in range(64)),
                                client.publish("news", b"hello"),
                            )
                            self.assertEqual(
                                results[:-1], [str(i % 16).encode() for i in range(64)]
                            )
                            self.assertEqual(results[-1], 1)
                            self.assertEqual(
                                await client.next_message(), Message("news", b"hello")
                            )
                finally:
                    await server.close()

    async def test_timed_out_request_keeps_tcp_session_usable_for_reordered_replies(
        self,
    ) -> None:
        wire = protocol.DEFAULT_PROTOCOL
        first_seen, send_replies = asyncio.Event(), asyncio.Event()

        async def peer(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                first = await wire.read_command(reader)
                self.assertEqual(first, Request(1, Get("first")))
                first_seen.set()
                await send_replies.wait()
                second = await wire.read_command(reader)
                self.assertEqual(second, Request(2, Get("second")))
                writer.write(
                    wire.frames.encode(Response(second.request_id, Value(b"current")))
                    + wire.frames.encode(Message("news", b"push"))
                    + wire.frames.encode(Response(first.request_id, Value(b"late")))
                )
                await writer.drain()
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()

        async with asyncio.TaskGroup() as tasks:

            def accept(reader, writer):
                tasks.create_task(peer(reader, writer))

            listener = await asyncio.start_server(accept, "127.0.0.1", 0)
            try:
                port = listener.sockets[0].getsockname()[1]
                async with await Client.connect("127.0.0.1", port) as client:
                    with self.assertRaises(TimeoutError):
                        async with asyncio.timeout(0.05):
                            await client.get("first")
                    self.assertTrue(first_seen.is_set())
                    send_replies.set()
                    async with asyncio.timeout(1):
                        self.assertEqual(await client.get("second"), b"current")
                        self.assertEqual(
                            await client.next_message(), Message("news", b"push")
                        )
            finally:
                listener.close()
                listener.close_clients()
                await listener.wait_closed()
