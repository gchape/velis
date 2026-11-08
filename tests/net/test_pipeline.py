import asyncio
import contextlib
import unittest

from tests.codec.helpers import Reverse
from velis.codec import CompositeCodec
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
from velis.net.server import TcpServer
from velis.net.session import SessionLimits


class PipelineIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.store = Store()
        self.engine = Engine(self.store, Broker())
        self.server = TcpServer(self.engine, ServerConfig("127.0.0.1", 0))
        await self.server.start()
        self.addAsyncCleanup(self.server.close)

    async def test_raw_pipeline_replies_in_order_including_command_errors(self) -> None:
        wire = protocol.DEFAULT_PROTOCOL
        requests = (
            Set("key", b"value"),
            Get("key"),
            Expire("key", MAX_TTL_MS + 1),
            Ttl("key"),
        )
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        try:
            writer.write(
                b"".join(wire.commands.encode(request) for request in requests)
            )
            async with asyncio.timeout(1):
                frames = wire.read_frames(reader)
                await self.enterAsyncContext(contextlib.aclosing(frames))
                responses = [await anext(frames) for _ in requests]
            self.assertEqual(
                responses,
                [
                    Ok(),
                    Value(b"value"),
                    Err("ttl out of range"),
                    Persistent(),
                ],
            )
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_concurrent_commands_and_pushes_with_default_and_custom_wires(
        self,
    ) -> None:
        transformed = protocol.WireProtocol(
            command_codec=CompositeCodec.of(protocol.COMMANDS) | Reverse(),
            frame_codec=CompositeCodec.of(protocol.FRAMES) | Reverse(),
        )
        for i in range(16):
            self.store.set(str(i), str(i).encode())
        for wire in (protocol.DEFAULT_PROTOCOL, transformed):
            with self.subTest(wire=wire):
                server = TcpServer(self.engine, ServerConfig("127.0.0.1", 0), wire=wire)
                await server.start()
                try:
                    async with await Client.connect(
                        "127.0.0.1",
                        server.port,
                        wire=wire,
                        limits=SessionLimits(max_pending_requests=3),
                    ) as client:
                        await client.subscribe("news")
                        async with asyncio.timeout(5):
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

    async def test_timed_out_request_keeps_tcp_session_usable_for_late_replies(
        self,
    ) -> None:
        wire = protocol.DEFAULT_PROTOCOL
        first_seen, send_replies = asyncio.Event(), asyncio.Event()

        async def peer(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                commands = wire.read_commands(reader)
                first = await anext(commands)
                self.assertEqual(first, Get("first"))
                first_seen.set()
                await send_replies.wait()
                second = await anext(commands)
                self.assertEqual(second, Get("second"))
                writer.write(
                    wire.frames.encode(Value(b"late"))
                    + wire.frames.encode(Message("news", b"push"))
                    + wire.frames.encode(Value(b"current"))
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
