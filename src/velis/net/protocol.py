"""Encode and decode commands, replies and server pushes.

String and bytes fields have a length prefix and a 1 MiB limit.
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


async def read_u32(reader: asyncio.StreamReader) -> int:
    """Read an unsigned big-endian 32-bit integer."""
    return _U32.unpack(await reader.readexactly(_U32.size))[0]


async def read_u64(reader: asyncio.StreamReader) -> int:
    """Read an unsigned big-endian 64-bit integer."""
    return _U64.unpack(await reader.readexactly(_U64.size))[0]


async def read_bytes(reader: asyncio.StreamReader) -> bytes:
    """Read a length-prefixed byte field, rejecting lengths above 1 MiB."""
    length = await read_u32(reader)
    if length > MAX_LEN:
        raise ProtocolError(f"field too long: {length}")
    return await reader.readexactly(length)


async def read_str(reader: asyncio.StreamReader) -> str:
    """Read a length-prefixed UTF-8 string, rejecting invalid encoding."""
    raw = await read_bytes(reader)
    try:
        return raw.decode()
    except UnicodeDecodeError:
        raise ProtocolError("invalid utf-8 in string field") from None


def _encode_bytes(data: bytes) -> bytes:
    """Prefix a byte field with its length, enforcing the field size limit."""
    if len(data) > MAX_LEN:
        raise ProtocolError(f"field too long: {len(data)}")
    return _U32.pack(len(data)) + data


def _encode_text(text: str) -> bytes:
    """Encode a UTF-8 field with the same length limit as a byte field."""
    try:
        raw = text.encode()
    except UnicodeEncodeError:
        raise ProtocolError("invalid utf-8 in string field") from None
    return _encode_bytes(raw)


def _encode_uint(value: int, layout: struct.Struct) -> bytes:
    """Encode an unsigned integer, rejecting values outside the wire range."""
    bits = layout.size * 8
    if not 0 <= value < 1 << bits:
        raise ProtocolError(f"unsigned {bits}-bit integer out of range: {value}")
    return layout.pack(value)


async def read_command(reader: asyncio.StreamReader) -> Command:
    """Decode a client command, rejecting unknown command names."""
    name = await read_str(reader)
    match name:
        case "GET":
            return Get(await read_str(reader))
        case "SET":
            key = await read_str(reader)
            return Set(key, await read_bytes(reader))
        case "EXPIRE":
            key = await read_str(reader)
            return Expire(key, await read_u64(reader))
        case "SUBSCRIBE":
            return Subscribe(await read_str(reader))
        case "UNSUBSCRIBE":
            return Unsubscribe(await read_str(reader))
        case "PUBLISH":
            channel = await read_str(reader)
            return Publish(channel, await read_bytes(reader))
        case _:
            raise ProtocolError(f"unknown command: {name!r}")


def encode_reply(reply: Reply) -> bytes:
    """Encode a command reply for the client."""
    match reply:
        case Ok():
            return _encode_text("OK")
        case Nil():
            return _encode_text("NIL")
        case Value(value):
            return _encode_text("VALUE") + _encode_bytes(value)
        case Err(message):
            return _encode_text("ERR") + _encode_text(message)
        case Subscribed(channel):
            return _encode_text("SUBSCRIBED") + _encode_text(channel)
        case Unsubscribed(channel):
            return _encode_text("UNSUBSCRIBED") + _encode_text(channel)
        case Delivered(count):
            return _encode_text("DELIVERED") + _encode_uint(count, _U32)
        case _:
            assert_never(reply)


def encode_message(channel: str, payload: bytes) -> bytes:
    """Encode a server push independently of command replies."""
    return _encode_text("MESSAGE") + _encode_text(channel) + _encode_bytes(payload)


def message(channel: str, payload: bytes) -> bytes:
    """Compatibility entry point for encode_message."""
    return encode_message(channel, payload)


def encode_command(command: Command) -> bytes:
    """Encode a client command and its fields for the server."""
    match command:
        case Get(key):
            return _encode_text("GET") + _encode_text(key)
        case Set(key, value):
            return _encode_text("SET") + _encode_text(key) + _encode_bytes(value)
        case Expire(key, ms):
            return _encode_text("EXPIRE") + _encode_text(key) + _encode_uint(ms, _U64)
        case Subscribe(channel):
            return _encode_text("SUBSCRIBE") + _encode_text(channel)
        case Unsubscribe(channel):
            return _encode_text("UNSUBSCRIBE") + _encode_text(channel)
        case Publish(channel, payload):
            return (
                _encode_text("PUBLISH") + _encode_text(channel) + _encode_bytes(payload)
            )
        case _:
            assert_never(command)


async def read_frame(reader: asyncio.StreamReader) -> Reply | Message:
    """Decode a server reply or pushed message, rejecting unknown tags."""
    tag = await read_str(reader)
    match tag:
        case "OK":
            return Ok()
        case "NIL":
            return Nil()
        case "VALUE":
            return Value(await read_bytes(reader))
        case "ERR":
            return Err(await read_str(reader))
        case "SUBSCRIBED":
            return Subscribed(await read_str(reader))
        case "UNSUBSCRIBED":
            return Unsubscribed(await read_str(reader))
        case "DELIVERED":
            return Delivered(await read_u32(reader))
        case "MESSAGE":
            channel = await read_str(reader)
            return Message(channel, await read_bytes(reader))
        case _:
            raise ProtocolError(f"unknown frame: {tag!r}")
