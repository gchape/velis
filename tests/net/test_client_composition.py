import unittest

from tests.codec.helpers import Reverse
from tests.helpers import RecordingSubscriber
from velis.core.broker import Broker
from velis.core.commands import Command, Err, Message, Reply
from velis.core.engine import Engine
from velis.core.store import Store
from velis.errors import CommandError
from velis.net.client import Client


class LocalSession:
    """Exercise the client against an engine without sockets or wire encoding."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.subscriber = RecordingSubscriber()
        self.closed = False

    async def request(self, command: Command) -> Reply:
        if self.closed:
            raise ConnectionError("session closed")
        try:
            return self.engine.execute(command, self.subscriber)
        except CommandError as exc:
            return Err(str(exc))

    async def next_message(self) -> Message:
        return Message(*self.subscriber.messages.pop(0))

    async def close(self) -> None:
        self.engine.disconnect(self.subscriber)
        self.closed = True


class ClientCompositionTests(unittest.IsolatedAsyncioTestCase):
    async def test_commands_and_codecs_work_over_an_independent_session(self) -> None:
        store = Store()
        session = LocalSession(Engine(store, Broker()))
        async with Client(session, Reverse()) as client:
            await client.set("key", b"abc")
            self.assertEqual(store.get("key"), b"cba")
            self.assertEqual(await client.get("key"), b"abc")
            await client.subscribe("news")
            self.assertEqual(await client.publish("news", b"hello"), 1)
            self.assertEqual(await client.next_message(), Message("news", b"hello"))
        self.assertTrue(session.closed)

    async def test_error_replies_are_translated_by_client(self) -> None:
        session = LocalSession(Engine(Store(), Broker()))
        async with Client(session) as client:
            with self.assertRaisesRegex(CommandError, "ttl out of range"):
                await client.expire("key", -1)
            await client.set("key", b"still usable")
            self.assertEqual(await client.get("key"), b"still usable")

    async def test_context_exit_closes_session_on_caller_failure(self) -> None:
        session = LocalSession(Engine(Store(), Broker()))
        with self.assertRaisesRegex(ValueError, "caller failed"):
            async with Client(session):
                raise ValueError("caller failed")
        self.assertTrue(session.closed)
