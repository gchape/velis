import asyncio
import struct
import unittest

from tests.codec.helpers import Reverse
from velis.codec import CodecChain
from velis.config import Server as ServerConfig
from velis.core.broker import Broker
from velis.core.commands import Err, Message
from velis.core.engine import Engine
from velis.core.store import Store
from velis.errors import CommandError
from velis.net import protocol
from velis.net.client import Client
from velis.net.server import TcpServer


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.engine = Engine(Store(), Broker())
        self.server = TcpServer(self.engine, ServerConfig("127.0.0.1", 0))
        await self.server.start()
        self.addAsyncCleanup(self.server.close)

    async def client(self, codec: CodecChain | None = None) -> Client:
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
        writer.write(struct.pack(">I", 4) + b"NOPE")
        frame = await asyncio.wait_for(protocol.read_frame(reader), 1)
        self.assertIsInstance(frame, Err)
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
        coded = await self.client(CodecChain.of(Reverse()))
        raw = await self.client()
        await coded.set("k", b"abc")
        self.assertEqual(await raw.get("k"), b"cba")
        self.assertEqual(await coded.get("k"), b"abc")


if __name__ == "__main__":
    unittest.main()
