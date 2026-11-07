"""Encode and decode commands, replies and server pushes.

String and bytes fields have a length prefix and a 1 MiB receive limit.
Integers use big-endian byte order.
"""

from __future__ import annotations

import asyncio
import struct
from typing import assert_never

from velis.core.commands import (
    Command,
    Delivered,
    Err,
    Expire,
    Get,
    Message,
    Nil,
    Ok,
    Publish,
    Reply,
    Set,
    Subscribe,
    Subscribed,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.errors import ProtocolError

_U32 = struct.Struct(">I")
_U64 = struct.Struct(">Q")
MAX_LEN = 1 << 20


async def read_u32(r: asyncio.StreamReader) -> int:
    """Read an unsigned big-endian 32-bit integer."""
    return _U32.unpack(await r.readexactly(_U32.size))[0]


async def read_u64(r: asyncio.StreamReader) -> int:
    """Read an unsigned big-endian 64-bit integer."""
    return _U64.unpack(await r.readexactly(_U64.size))[0]


async def read_bytes(r: asyncio.StreamReader) -> bytes:
    """Read a length-prefixed byte field, rejecting lengths above 1 MiB."""
    n = await read_u32(r)
    if n > MAX_LEN:
        raise ProtocolError(f"field too long: {n}")
    return await r.readexactly(n)


async def read_str(r: asyncio.StreamReader) -> str:
    """Read a length-prefixed UTF-8 string, rejecting invalid encoding."""
    raw = await read_bytes(r)
    try:
        return raw.decode()
    except UnicodeDecodeError:
        raise ProtocolError("invalid utf-8 in string field") from None


def _bytes(b: bytes) -> bytes:
    return _U32.pack(len(b)) + b


def _str(s: str) -> bytes:
    return _bytes(s.encode())


async def read_command(r: asyncio.StreamReader) -> Command:
    """Decode a client command, rejecting unknown command names."""
    name = await read_str(r)
    match name:
        case "GET":
            return Get(await read_str(r))
        case "SET":
            key = await read_str(r)
            return Set(key, await read_bytes(r))
        case "EXPIRE":
            key = await read_str(r)
            return Expire(key, await read_u64(r))
        case "SUBSCRIBE":
            return Subscribe(await read_str(r))
        case "UNSUBSCRIBE":
            return Unsubscribe(await read_str(r))
        case "PUBLISH":
            channel = await read_str(r)
            return Publish(channel, await read_bytes(r))
        case _:
            raise ProtocolError(f"unknown command: {name!r}")


def encode_reply(reply: Reply) -> bytes:
    """Encode a command reply for the client."""
    match reply:
        case Ok():
            return _str("OK")
        case Nil():
            return _str("NIL")
        case Value(value):
            return _str("VALUE") + _bytes(value)
        case Err(message):
            return _str("ERR") + _str(message)
        case Subscribed(channel):
            return _str("SUBSCRIBED") + _str(channel)
        case Unsubscribed(channel):
            return _str("UNSUBSCRIBED") + _str(channel)
        case Delivered(count):
            return _str("DELIVERED") + _U32.pack(count)
        case _:
            assert_never(reply)


def message(channel: str, payload: bytes) -> bytes:
    """Encode a server push independently of command replies."""
    return _str("MESSAGE") + _str(channel) + _bytes(payload)


def encode_command(cmd: Command) -> bytes:
    """Encode a client command and its fields for the server."""
    match cmd:
        case Get(key):
            return _str("GET") + _str(key)
        case Set(key, value):
            return _str("SET") + _str(key) + _bytes(value)
        case Expire(key, ms):
            return _str("EXPIRE") + _str(key) + _U64.pack(ms)
        case Subscribe(channel):
            return _str("SUBSCRIBE") + _str(channel)
        case Unsubscribe(channel):
            return _str("UNSUBSCRIBE") + _str(channel)
        case Publish(channel, payload):
            return _str("PUBLISH") + _str(channel) + _bytes(payload)
        case _:
            assert_never(cmd)


async def read_frame(r: asyncio.StreamReader) -> Reply | Message:
    """Decode a server reply or pushed message, rejecting unknown tags."""
    tag = await read_str(r)
    match tag:
        case "OK":
            return Ok()
        case "NIL":
            return Nil()
        case "VALUE":
            return Value(await read_bytes(r))
        case "ERR":
            return Err(await read_str(r))
        case "SUBSCRIBED":
            return Subscribed(await read_str(r))
        case "UNSUBSCRIBED":
            return Unsubscribed(await read_str(r))
        case "DELIVERED":
            return Delivered(await read_u32(r))
        case "MESSAGE":
            channel = await read_str(r)
            return Message(channel, await read_bytes(r))
        case _:
            raise ProtocolError(f"unknown frame: {tag!r}")
