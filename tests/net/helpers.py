import asyncio

from velis.app import build_engine
from velis.core.engine import Engine
from velis.core.messages import Command
from velis.net import protocol
from velis.net.connection import Connection
from velis.net.protocol import ServerFrame


def make_connection(
    transport: asyncio.Transport,
    *,
    engine: Engine | None = None,
    max_buffer: int = 16,
) -> Connection:
    connection = Connection(
        build_engine() if engine is None else engine,
        max_buffer,
        admit=lambda: True,
        release=lambda: None,
    )
    connection.connection_made(transport)
    return connection


def decode_command_frame(data: bytes) -> Command:
    if int.from_bytes(data[:4]) != len(data) - 4:
        raise AssertionError("expected one complete command frame")
    return protocol.COMMANDS.decode(data[4:])


def decode_message_frame(data: bytes) -> ServerFrame:
    if int.from_bytes(data[:4]) != len(data) - 4:
        raise AssertionError("expected one complete server frame")
    return protocol.SERVER_MESSAGES.decode(data[4:])


async def read_message(reader: asyncio.StreamReader) -> ServerFrame:
    body = await protocol.read_frame(reader)
    if body is None:
        raise AssertionError("expected a server message before EOF")
    return protocol.SERVER_MESSAGES.decode(body)
