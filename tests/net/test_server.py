import asyncio
import struct
import unittest
from unittest.mock import Mock, patch

from tests.helpers import ManualClock
from tests.net.helpers import make_connection, read_message
from velis.app import serve
from velis.config import ServerLimits
from velis.core.broker import Broker
from velis.core.engine import Engine
from velis.core.eviction import EvictionPolicy
from velis.core.messages import Err, Message, Subscribe
from velis.core.store import Store, StoreLimits
from velis.errors import CommandError, ProtocolError
from velis.net import protocol
from velis.net.client import Client
from velis.net.limits import MAX_FIELD_BYTES
from velis.net.server import TCPServer


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_binary_values_and_publications_remain_opaque(self) -> None:
        client = await self.client()
        await client.subscribe("binary")
        for payload in (b"", bytes(range(256))):
            with self.subTest(size=len(payload)):
                await client.set("key", payload)
                self.assertEqual(await client.get("key"), payload)
                self.assertEqual(await client.publish("binary", payload), 1)
                async with asyncio.timeout(1):
                    self.assertEqual(
                        await client.next_message(), Message("binary", payload)
                    )

    async def test_slow_subscriber_is_aborted_without_blocking_publisher(self) -> None:
        transport = Mock(spec=asyncio.Transport)
        transport.is_closing.return_value = False
        transport.get_write_buffer_size.return_value = 17
        slow = make_connection(transport, max_buffer=16)
        self.engine.execute(Subscribe("news"), slow)
        healthy, publisher = await self.client(), await self.client()
        await healthy.subscribe("news")
        async with asyncio.timeout(1):
            self.assertEqual(await publisher.publish("news", b"hello"), 1)
            self.assertEqual(await healthy.next_message(), Message("news", b"hello"))
            self.assertEqual(await publisher.publish("news", b"again"), 1)
        transport.abort.assert_called_once()

    async def test_connection_limit_reason_reaches_idle_client(self) -> None:
        server = TCPServer(self.engine, ServerLimits("127.0.0.1", 0, max_connections=1))
        await server.start()
        self.addAsyncCleanup(server.close)
        first = await Client.connect("127.0.0.1", server.port)
        self.addAsyncCleanup(first.close)
        await first.set("key", b"value")
        rejected = await Client.connect("127.0.0.1", server.port)
        self.addAsyncCleanup(rejected.close)
        async with asyncio.timeout(1):
            with self.assertRaisesRegex(ConnectionError, "connection limit exceeded"):
                await rejected.next_message()
            with self.assertRaisesRegex(ConnectionError, "connection limit exceeded"):
                await rejected.get("key")
            self.assertEqual(await first.get("key"), b"value")

    async def test_connection_limit_rejects_excess_and_reuses_closed_slot(self) -> None:
        server = TCPServer(self.engine, ServerLimits("127.0.0.1", 0, max_connections=1))
        await server.start()
        self.addAsyncCleanup(server.close)
        first = await Client.connect("127.0.0.1", server.port)
        self.addAsyncCleanup(first.close)
        await first.set("key", b"value")
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            async with asyncio.timeout(1):
                self.assertEqual(
                    await read_message(reader),
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
        self.server = TCPServer(self.engine, ServerLimits("127.0.0.1", 0))
        await self.server.start()
        self.addAsyncCleanup(self.server.close)

    async def client(self) -> Client:
        c = await Client.connect("127.0.0.1", self.server.port)
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
            body = b"\x00\x00\x00\x03GET" + struct.pack(">I", 0)
            writer.write(protocol.encode_frame(body))
            async with asyncio.timeout(1):
                self.assertEqual(
                    await read_message(reader),
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
        big = b"x" * MAX_FIELD_BYTES
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
        writer.write(protocol.encode_frame(struct.pack(">I", 4) + b"NOPE"))
        frame = await asyncio.wait_for(read_message(reader), 1)
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


class RuntimeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_s3fifo_eviction_over_tcp(self) -> None:
        async with (
            serve(
                ServerLimits("127.0.0.1", 0),
                store_limits=StoreLimits(max_keys=2, max_bytes=4),
            ) as server,
            await Client.connect("127.0.0.1", server.port) as client,
        ):
            await client.set("a", b"1")
            await client.expire("a", 60_000)
            await client.set("b", b"2")
            self.assertEqual(await client.get("b"), b"2")
            await client.set("c", b"3")
            self.assertIsNone(await client.get("a"))
            self.assertEqual(await client.ttl("a"), -2)
            with self.assertRaisesRegex(CommandError, "byte limit"):
                await client.set("b", b"oversized")
            self.assertEqual(await client.get("b"), b"2")
            self.assertEqual(await client.get("c"), b"3")

    async def test_delete_ttl_and_persist_over_tcp(self) -> None:
        clock = ManualClock()
        store = Store(clock, limits=StoreLimits(max_keys=1))
        engine = Engine(store, Broker())
        async with (
            serve(ServerLimits("127.0.0.1", 0), engine=engine) as server,
            await Client.connect("127.0.0.1", server.port) as client,
        ):
            self.assertEqual(await client.ttl("k"), -2)
            self.assertFalse(await client.delete("k"))
            self.assertFalse(await client.persist("k"))
            await client.set("k", b"")
            self.assertEqual(await client.ttl("k"), -1)
            self.assertFalse(await client.persist("k"))
            await client.expire("k", 100)
            self.assertEqual(await client.ttl("k"), 100)
            clock.advance(99)
            self.assertEqual(await client.ttl("k"), 1)
            self.assertTrue(await client.persist("k"))
            clock.advance(1)
            self.assertEqual(await client.get("k"), b"")
            self.assertEqual(await client.ttl("k"), -1)
            self.assertTrue(await client.delete("k"))
            await client.set("other", b"v")
            await client.expire("other", 0)
            self.assertFalse(await client.persist("other"))
            self.assertEqual(await client.ttl("other"), -2)
            self.assertFalse(await client.delete("other"))
            for operation in (client.delete, client.ttl, client.persist):
                with self.assertRaises(ProtocolError):
                    await operation("")
            await client.set("k", b"still usable")
            self.assertEqual(await client.get("k"), b"still usable")

    async def test_capacity_errors_keep_connection_and_existing_state_usable(
        self,
    ) -> None:
        async with (
            serve(
                ServerLimits("127.0.0.1", 0),
                store_limits=StoreLimits(max_keys=1, max_bytes=4),
                max_subscriptions=1,
            ) as server,
            await Client.connect("127.0.0.1", server.port) as client,
        ):
            await client.set("k", b"old")
            await client.expire("k", 60_000)
            with self.assertRaisesRegex(CommandError, "byte limit"):
                await client.set("x", b"oversized")
            with self.assertRaisesRegex(CommandError, "byte limit"):
                await client.set("k", b"large")
            self.assertEqual(await client.get("k"), b"old")
            self.assertGreater(await client.ttl("k"), 0)
            await client.subscribe("a")
            await client.subscribe("a")
            with self.assertRaisesRegex(CommandError, "subscription limit"):
                await client.subscribe("b")
            await client.unsubscribe("a")
            await client.subscribe("b")
            self.assertEqual(await client.publish("b", b"v"), 1)
            async with asyncio.timeout(1):
                self.assertEqual(await client.next_message(), Message("b", b"v"))

    async def test_lfu_eviction_and_deadline_cleanup_over_tcp(self) -> None:
        async with (
            serve(
                ServerLimits("127.0.0.1", 0),
                store_limits=StoreLimits(
                    max_keys=2, eviction_policy=EvictionPolicy.LFU
                ),
            ) as server,
            await Client.connect("127.0.0.1", server.port) as client,
        ):
            await client.set("a", b"first")
            await client.set("b", b"second")
            await client.expire("b", 60_000)
            self.assertEqual(await client.get("a"), b"first")
            await client.set("c", b"third")
            self.assertIsNone(await client.get("b"))
            self.assertEqual(await client.ttl("b"), -2)
            self.assertEqual(await client.get("a"), b"first")
            self.assertEqual(await client.get("c"), b"third")

    async def test_server_encodes_one_push_per_publish_for_all_subscribers(
        self,
    ) -> None:
        codec = self.enterContext(
            patch.object(
                protocol, "SERVER_MESSAGES", Mock(wraps=protocol.SERVER_MESSAGES)
            )
        )
        async with (
            serve(ServerLimits("127.0.0.1", 0)) as server,
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
        async with (
            serve(ServerLimits("127.0.0.1", 0), engine=engine) as server,
            await Client.connect("127.0.0.1", server.port) as client,
        ):
            self.assertEqual(await client.get("existing"), b"value")
            await client.expire("existing", 500)
            clock.advance(500)
            self.assertIsNone(await client.get("existing"))
            await client.set("retained", b"data")
        self.assertEqual(store.get("retained"), b"data")

    async def test_runtime_scope_serves_and_closes_live_connections(self) -> None:
        async with serve(ServerLimits("127.0.0.1", 0)) as server:
            client = await Client.connect("127.0.0.1", server.port)
            self.addAsyncCleanup(client.close)
            await client.set("key", b"value")
            self.assertEqual(await client.get("key"), b"value")
        async with asyncio.timeout(1):
            with self.assertRaises(ConnectionError):
                await client.get("key")


if __name__ == "__main__":
    unittest.main()
