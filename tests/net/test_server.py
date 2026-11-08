import asyncio
import struct
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests.codec.helpers import Reverse
from tests.helpers import ManualClock
from velis.app import serve
from velis.codec import Codec, CodecChain, Validate
from velis.config import Cache, Config
from velis.config import Server as ServerConfig
from velis.core.broker import Broker
from velis.core.commands import Command, Err, Get, Message, Subscribe
from velis.core.engine import Engine
from velis.core.store import Store
from velis.errors import CommandError, ProtocolError
from velis.net import protocol
from velis.net.client import Client
from velis.net.connection import Connection
from velis.net.server import TcpServer


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_subscriber_is_aborted_without_blocking_publisher(self) -> None:
        writer = Mock(spec=asyncio.StreamWriter)
        writer.is_closing.return_value = False
        writer.transport = Mock(spec=asyncio.Transport)
        writer.transport.get_write_buffer_size.return_value = 17
        writer.drain = AsyncMock(
            side_effect=AssertionError("must not drain subscriber")
        )
        slow = Connection(Mock(spec=asyncio.StreamReader), writer, max_buffer=16)
        self.engine.execute(Subscribe("news"), slow)
        healthy, publisher = await self.client(), await self.client()
        await healthy.subscribe("news")
        async with asyncio.timeout(1):
            self.assertEqual(await publisher.publish("news", b"hello"), 1)
            self.assertEqual(await healthy.next_message(), Message("news", b"hello"))
            self.assertEqual(await publisher.publish("news", b"again"), 1)
        writer.transport.abort.assert_called_once()
        writer.drain.assert_not_awaited()

    async def test_connection_limit_rejects_excess_and_reuses_closed_slot(self) -> None:
        server = TcpServer(self.engine, ServerConfig("127.0.0.1", 0, max_connections=1))
        await server.start()
        self.addAsyncCleanup(server.close)
        first = await Client.connect("127.0.0.1", server.port)
        self.addAsyncCleanup(first.close)
        await first.set("key", b"value")
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            async with asyncio.timeout(1):
                self.assertEqual(
                    await protocol.DEFAULT_PROTOCOL.read_frame(reader),
                    Err("connection limit exceeded"),
                )
                self.assertEqual(await reader.read(), b"")
            self.assertEqual(await first.get("key"), b"value")
        finally:
            writer.close()
            await writer.wait_closed()
        disconnected = asyncio.Event()
        disconnect = self.engine.disconnect

        def on_disconnect(subscriber) -> None:
            disconnect(subscriber)
            disconnected.set()

        with patch.object(self.engine, "disconnect", side_effect=on_disconnect):
            await first.close()
            async with asyncio.timeout(1):
                await disconnected.wait()
        async with asyncio.timeout(1):
            async with await Client.connect("127.0.0.1", server.port) as replacement:
                self.assertEqual(await replacement.get("key"), b"value")

    async def asyncSetUp(self) -> None:
        self.engine = Engine(Store(), Broker())
        self.server = TcpServer(self.engine, ServerConfig("127.0.0.1", 0))
        await self.server.start()
        self.addAsyncCleanup(self.server.close)

    async def client(self, codec: Codec[bytes, bytes] | None = None) -> Client:
        c = await Client.connect("127.0.0.1", self.server.port, codec)
        self.addAsyncCleanup(c.close)
        return c

    async def test_set_get_expire(self) -> None:
        c = await self.client()
        await c.set("k", b"v")
        self.assertEqual(await c.get("k"), b"v")
        self.assertTrue(await c.expire("k", 60_000))
        self.assertFalse(await c.expire("missing", 1))
        self.assertIsNone(await c.get("missing"))

    async def test_invalid_name_rejected_locally_keeps_connection_usable(self) -> None:
        client = await self.client()
        await client.set("k", b"v")
        with self.assertRaisesRegex(ProtocolError, "key must not be empty"):
            await client.get("")
        self.assertEqual(await client.get("k"), b"v")

    async def test_empty_key_from_raw_socket_gets_err_then_eof(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        try:
            body = protocol.TEXT.encode("GET") + struct.pack(">I", 0)
            writer.write(protocol.DEFAULT_PROTOCOL.framing.encode(body))
            async with asyncio.timeout(1):
                self.assertEqual(
                    await protocol.DEFAULT_PROTOCOL.read_frame(reader),
                    Err("key must not be empty"),
                )
                self.assertEqual(await reader.read(), b"")
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_pubsub(self) -> None:
        sub, pub = await self.client(), await self.client()
        await sub.subscribe("ch")
        self.assertEqual(await pub.publish("ch", b"hi"), 1)
        async with asyncio.timeout(1):
            self.assertEqual(await sub.next_message(), Message("ch", b"hi"))

    async def test_message_timeout_allows_later_delivery(self) -> None:
        sub, pub = await self.client(), await self.client()
        await sub.subscribe("ch")
        with self.assertRaises(TimeoutError):
            async with asyncio.timeout(0.01):
                await sub.next_message()
        self.assertEqual(await pub.publish("ch", b"after timeout"), 1)
        async with asyncio.timeout(1):
            self.assertEqual(await sub.next_message(), Message("ch", b"after timeout"))

    async def test_max_size_message_does_not_drop_subscriber(self) -> None:
        sub, pub = await self.client(), await self.client()
        await sub.subscribe("ch")
        big = b"x" * protocol.MAX_LEN
        self.assertEqual(await pub.publish("ch", big), 1)
        async with asyncio.timeout(5):
            self.assertEqual((await sub.next_message()).payload, big)
        self.assertEqual(await pub.publish("ch", b"again"), 1)

    async def test_command_error_keeps_connection(self) -> None:
        c = await self.client()
        await c.set("k", b"v")
        with self.assertRaises(CommandError):
            await c.expire("k", 1 << 63)
        self.assertEqual(await c.get("k"), b"v")

    async def test_unknown_command_gets_err_then_close(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        writer.write(
            protocol.DEFAULT_PROTOCOL.framing.encode(struct.pack(">I", 4) + b"NOPE")
        )
        frame = await asyncio.wait_for(protocol.DEFAULT_PROTOCOL.read_frame(reader), 1)
        self.assertEqual(frame, Err("unknown command: 'NOPE'"))
        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
        writer.close()

    async def test_disconnect_removes_subscription(self) -> None:
        sub, pub = await self.client(), await self.client()
        await sub.subscribe("ch")
        await sub.close()
        for _ in range(50):
            if await pub.publish("ch", b"x") == 0:
                return
            await asyncio.sleep(0.02)
        self.fail("subscriber was never cleaned up")

    async def test_codec_lives_on_the_client(self) -> None:
        """The server stores opaque bytes while the client applies its codec."""
        coded = await self.client(Reverse())
        raw = await self.client()
        await coded.set("k", b"abc")
        self.assertEqual(await raw.get("k"), b"cba")
        self.assertEqual(await coded.get("k"), b"abc")

    async def test_custom_wire_chains_cover_commands_replies_pushes_and_errors(
        self,
    ) -> None:
        wire = protocol.WireProtocol(
            command_codec=CodecChain.of(protocol.CommandCodec()) | Reverse(),
            frame_codec=CodecChain.of(protocol.FrameCodec()) | Reverse(),
        )
        store = Store()
        server = TcpServer(
            Engine(store, Broker()), ServerConfig("127.0.0.1", 0), wire=wire
        )
        await server.start()
        self.addAsyncCleanup(server.close)
        async with await Client.connect(
            "127.0.0.1", server.port, codec=Reverse(), wire=wire
        ) as client:
            await client.set("key", b"value")
            self.assertEqual(store.get("key"), b"eulav")
            self.assertEqual(await client.get("key"), b"value")
            await client.subscribe("news")
            self.assertEqual(await client.publish("news", b"hello"), 1)
            async with asyncio.timeout(1):
                self.assertEqual(await client.next_message(), Message("news", b"hello"))
            with self.assertRaises(CommandError):
                await client.expire("key", 1 << 63)
            self.assertEqual(await client.get("key"), b"value")
            await client.unsubscribe("news")

    async def test_command_rule_rejects_locally_without_writing(self) -> None:
        def allow(command: Command) -> None:
            if isinstance(command, Get) and command.key == "blocked":
                raise ProtocolError("blocked key")

        wire = protocol.WireProtocol(
            command_codec=CodecChain.of(Validate("allowed", allow))
            | protocol.CommandCodec(),
        )
        async with await Client.connect(
            "127.0.0.1", self.server.port, wire=wire
        ) as client:
            with self.assertRaisesRegex(ProtocolError, "blocked key"):
                await client.get("blocked")
            await client.set("key", b"value")
            self.assertEqual(await client.get("key"), b"value")

    async def test_command_rule_runs_on_server_decode(self) -> None:
        def allow(command: Command) -> None:
            if isinstance(command, Get) and command.key == "blocked":
                raise ProtocolError("blocked key")

        wire = protocol.WireProtocol(
            command_codec=CodecChain.of(Validate("allowed", allow))
            | protocol.CommandCodec(),
            frame_codec=CodecChain.of(protocol.FrameCodec()) | Reverse(),
        )
        server = TcpServer(self.engine, ServerConfig("127.0.0.1", 0), wire=wire)
        await server.start()
        self.addAsyncCleanup(server.close)
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            writer.write(protocol.DEFAULT_PROTOCOL.commands.encode(Get("blocked")))
            async with asyncio.timeout(1):
                self.assertEqual(await wire.read_frame(reader), Err("blocked key"))
                self.assertEqual(await reader.read(), b"")
        finally:
            writer.close()
            await writer.wait_closed()


class RuntimeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_capacity_errors_keep_connection_and_existing_state_usable(
        self,
    ) -> None:
        config = Config(
            "INFO",
            ServerConfig("127.0.0.1", 0, max_subscriptions=1),
            cache=Cache(max_keys=1, max_bytes=4),
        )
        async with (
            serve(config) as server,
            await Client.connect("127.0.0.1", server.port) as client,
        ):
            await client.set("k", b"old")
            await client.expire("k", 60_000)
            with self.assertRaisesRegex(CommandError, "key limit"):
                await client.set("other", b"")
            with self.assertRaisesRegex(CommandError, "byte limit"):
                await client.set("k", b"large")
            self.assertEqual(await client.get("k"), b"old")
            await client.subscribe("a")
            await client.subscribe("a")
            with self.assertRaisesRegex(CommandError, "subscription limit"):
                await client.subscribe("b")
            await client.unsubscribe("a")
            await client.subscribe("b")
            self.assertEqual(await client.publish("b", b"v"), 1)
            async with asyncio.timeout(1):
                self.assertEqual(await client.next_message(), Message("b", b"v"))

    async def test_server_encodes_one_push_per_publish_for_all_subscribers(
        self,
    ) -> None:
        codec = Mock(wraps=protocol.FrameCodec())
        wire = protocol.WireProtocol(frame_codec=codec)
        config = Config("INFO", ServerConfig("127.0.0.1", 0))
        async with (
            serve(config, wire=wire) as server,
            await Client.connect("127.0.0.1", server.port) as first,
            await Client.connect("127.0.0.1", server.port) as second,
            await Client.connect("127.0.0.1", server.port) as publisher,
        ):
            await first.subscribe("news")
            await second.subscribe("news")
            for _ in range(2):
                codec.encode.reset_mock()
                self.assertEqual(await publisher.publish("news", b"same payload"), 2)
                async with asyncio.timeout(1):
                    for subscriber in (first, second):
                        self.assertEqual(
                            await subscriber.next_message(),
                            Message("news", b"same payload"),
                        )
                pushes = [
                    call
                    for call in codec.encode.call_args_list
                    if isinstance(call.args[0], Message)
                ]
                self.assertEqual(len(pushes), 1)

    async def test_runtime_uses_supplied_engine_and_clock(self) -> None:
        clock = ManualClock()
        store = Store(clock)
        store.set("existing", b"value")
        engine = Engine(store, Broker())
        config = Config("INFO", ServerConfig("127.0.0.1", 0))
        async with (
            serve(config, engine=engine) as server,
            await Client.connect("127.0.0.1", server.port) as client,
        ):
            self.assertEqual(await client.get("existing"), b"value")
            await client.expire("existing", 500)
            clock.advance(500)
            self.assertIsNone(await client.get("existing"))
            await client.set("retained", b"data")
        self.assertEqual(store.get("retained"), b"data")

    async def test_runtime_scope_serves_and_closes_live_connections(self) -> None:
        config = Config("INFO", ServerConfig("127.0.0.1", 0))
        wire = protocol.WireProtocol(
            command_codec=CodecChain.of(protocol.CommandCodec()) | Reverse(),
            frame_codec=CodecChain.of(protocol.FrameCodec()) | Reverse(),
        )
        async with serve(config, wire=wire) as server:
            client = await Client.connect("127.0.0.1", server.port, wire=wire)
            self.addAsyncCleanup(client.close)
            await client.set("key", b"value")
            self.assertEqual(await client.get("key"), b"value")
        async with asyncio.timeout(1):
            with self.assertRaises(ConnectionError):
                await client.get("key")


if __name__ == "__main__":
    unittest.main()
