import asyncio
import contextlib
import random
import struct
import unittest
from collections.abc import AsyncGenerator, Callable

from tests.codec.helpers import Reverse
from velis.codec import (
    Codec,
    CompositeCodec,
    Identity,
    LengthPrefixedField,
    UInt,
)
from velis.core.messages import (
    TTL,
    Delete,
    Delivered,
    Err,
    Expire,
    Get,
    Message,
    Nil,
    Ok,
    Persist,
    Persistent,
    Publish,
    Set,
    Subscribe,
    Subscribed,
    TTLValue,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.errors import ProtocolError
from velis.net import protocol


def stream(data: bytes) -> asyncio.StreamReader:
    """Expose an encoded frame as a complete in-memory stream."""
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    def test_default_framing_roundtrips_large_payloads(self) -> None:
        payload = b"\x00\xff" * 8192
        wire = protocol.DEFAULT_PROTOCOL
        command = Set("key", payload)
        self.assertEqual(
            wire.framed_commands.decode(wire.framed_commands.encode(command)), command
        )
        for message in (Value(payload), Message("news", payload)):
            with self.subTest(message_type=type(message)):
                self.assertEqual(
                    wire.framed_messages.decode(wire.framed_messages.encode(message)),
                    message,
                )

    def test_custom_framing_validation_is_preserved(self) -> None:
        class RestrictedFraming(LengthPrefixedField[bytes]):
            def check_length(self, length: int) -> None:
                super().check_length(length)
                if length > 24:
                    raise ProtocolError("custom framing limit")

        wire = protocol.WireProtocol(
            framing=RestrictedFraming(Identity(), 100, prefix=UInt(4))
        )
        command = Get("a")
        self.assertEqual(
            wire.framed_commands.decode(wire.framed_commands.encode(command)), command
        )
        with self.assertRaisesRegex(ProtocolError, "custom framing limit"):
            wire.framed_commands.encode(Get("x" * 20))
        with self.assertRaisesRegex(ProtocolError, "custom framing limit"):
            wire.framed_messages.encode(Value(b"x" * 20))

    async def test_custom_outer_prefixes_match_sync_and_stream_decoding(self) -> None:
        for prefix_length in (1, 2, 3, 4, 8):
            for value_codec in (Identity(), Reverse()):
                with self.subTest(prefix_length=prefix_length, codec=value_codec):
                    framing = LengthPrefixedField(
                        value_codec, 255, prefix=UInt(prefix_length)
                    )
                    wire = protocol.WireProtocol(framing=framing)
                    commands = [Set("key", b"value"), Get("key")]
                    for command in commands:
                        self.assertEqual(
                            wire.framed_commands.decode(
                                wire.framed_commands.encode(command)
                            ),
                            command,
                        )
                    encoded = b"".join(
                        wire.framed_commands.encode(cmd) for cmd in commands
                    )
                    self.assertEqual(
                        [cmd async for cmd in wire.read_commands(stream(encoded))],
                        commands,
                    )
                    frames = [Value(b"value"), Message("news", b"hello")]
                    for frame in frames:
                        self.assertEqual(
                            wire.framed_messages.decode(
                                wire.framed_messages.encode(frame)
                            ),
                            frame,
                        )
                    encoded = b"".join(
                        wire.framed_messages.encode(frame) for frame in frames
                    )
                    self.assertEqual(
                        [frame async for frame in wire.read_messages(stream(encoded))],
                        frames,
                    )

    async def test_composite_body_codec_works_with_framing_and_streams(self) -> None:
        codec = CompositeCodec.of(protocol.COMMANDS) | Reverse()
        wire = protocol.WireProtocol(command_codec=codec)
        self.assertIsInstance(wire.framed_commands, CompositeCodec)
        self.assertIs(wire.framed_commands, wire.framed_commands)
        command = Set("key", b"hello")
        encoded = wire.framed_commands.encode(command)
        self.assertEqual(wire.framed_commands.decode(encoded), command)
        self.assertEqual(
            [value async for value in wire.read_commands(stream(encoded))], [command]
        )

    def test_arbitrary_and_mutated_bytes_only_raise_protocol_errors(self) -> None:
        rng = random.Random(0)

        def check[T](codec: Codec[T, bytes], seeds: tuple[T, ...]) -> None:
            encoded = tuple(codec.encode(value) for value in seeds)
            cases = [rng.randbytes(rng.randrange(256)) for _ in range(1000)]
            for body in encoded:
                for _ in range(100):
                    mutated = bytearray(body)
                    mutated[rng.randrange(len(mutated))] = rng.randrange(256)
                    cases.append(bytes(mutated))
                cases.extend(
                    body[:8] + rng.randbytes(rng.randrange(64)) for _ in range(50)
                )
            for data in cases:
                try:
                    value = codec.decode(data)
                except ProtocolError:
                    continue
                self.assertEqual(codec.decode(codec.encode(value)), value)

        commands = (
            Get("key"),
            Delete("key"),
            TTL("key"),
            Persist("key"),
            Set("key", b"\x00\xff"),
            Expire("key", 1),
            Subscribe("ch"),
            Unsubscribe("ch"),
            Publish("ch", b"hello"),
        )
        frames = (
            Ok(),
            TTLValue(1 << 62),
            Persistent(),
            Nil(),
            Value(b"\xff"),
            Err("error"),
            Subscribed("ch"),
            Unsubscribed("ch"),
            Delivered(42),
            Message("ch", b"hello"),
        )
        check(protocol.COMMANDS, commands)
        check(protocol.SERVER_MESSAGES, frames)
        check(protocol.DEFAULT_PROTOCOL.framed_commands, commands)
        check(protocol.DEFAULT_PROTOCOL.framed_messages, frames)

    def test_command_names_enforce_character_limits_in_both_directions(self) -> None:
        codec = protocol.COMMANDS
        for tag, make, limit, suffix in (
            ("GET", Get, 512, b""),
            ("DEL", Delete, 512, b""),
            ("TTL", TTL, 512, b""),
            ("PERSIST", Persist, 512, b""),
            ("SET", lambda name: Set(name, b"x"), 512, protocol.BYTES.encode(b"x")),
            ("EXPIRE", lambda name: Expire(name, 1), 512, struct.pack(">Q", 1)),
            ("SUBSCRIBE", Subscribe, 256, b""),
            ("UNSUBSCRIBE", Unsubscribe, 256, b""),
            (
                "PUBLISH",
                lambda name: Publish(name, b"x"),
                256,
                protocol.BYTES.encode(b"x"),
            ),
        ):
            for name in ("", "é" * (limit + 1)):
                with self.subTest(tag=tag, length=len(name)):
                    with self.assertRaises(ProtocolError):
                        codec.encode(make(name))
                    body = (
                        protocol.TEXT.encode(tag) + protocol.TEXT.encode(name) + suffix
                    )
                    with self.assertRaises(ProtocolError):
                        codec.decode(body)
            for name in ("é" * limit, "\U0010ffff" * limit, " \x00\n"):
                with self.subTest(tag=tag, name=name):
                    command = make(name)
                    body = (
                        protocol.TEXT.encode(tag) + protocol.TEXT.encode(name) + suffix
                    )
                    self.assertEqual(codec.encode(command), body)
                    self.assertEqual(codec.decode(body), command)

    def test_name_decoding_checks_length_then_utf8_then_rule(self) -> None:
        for tag, what, limit in (("GET", "key", 512), ("SUBSCRIBE", "channel", 256)):
            byte_limit = limit * 4
            for name, error in (
                (struct.pack(">I", byte_limit + 1), "field too long"),
                (protocol.BYTES.encode(b"\xff" * (byte_limit + 1)), "field too long"),
                (protocol.BYTES.encode(b"\xff" * byte_limit), "invalid utf-8"),
                (protocol.TEXT.encode("x" * (limit + 1)), f"{what} exceeds {limit}"),
                (protocol.TEXT.encode(""), f"{what} must not be empty"),
            ):
                with (
                    self.subTest(tag=tag, error=error),
                    self.assertRaisesRegex(ProtocolError, error),
                ):
                    protocol.COMMANDS.decode(protocol.TEXT.encode(tag) + name)

    def test_tags_reject_oversized_data_before_utf8_decoding(self) -> None:
        for codec in (protocol.COMMANDS, protocol.SERVER_MESSAGES):
            for tag, error in (
                (b"\xff" * 33, "field too long"),
                (b"\xff" * 32, "invalid utf-8"),
                (b"x" * 32, "unknown"),
            ):
                with (
                    self.subTest(codec=type(codec).__name__, tag=tag),
                    self.assertRaisesRegex(ProtocolError, error),
                ):
                    codec.decode(protocol.BYTES.encode(tag))

    def test_reply_channels_keep_existing_text_rules(self) -> None:
        codec = protocol.SERVER_MESSAGES
        for channel in ("", "x" * 257):
            for frame in (
                Message(channel, b"x"),
                Subscribed(channel),
                Unsubscribed(channel),
            ):
                with self.subTest(frame=frame):
                    self.assertEqual(codec.decode(codec.encode(frame)), frame)

    def test_body_codecs_reject_truncation_and_trailing_bytes(self) -> None:
        def check[T](codec: Codec[T, bytes], values: tuple[T, ...]) -> None:
            for value in values:
                body = codec.encode(value)
                for malformed in (
                    *[body[:end] for end in range(len(body))],
                    body + b"x",
                ):
                    with (
                        self.subTest(value=value, body=malformed),
                        self.assertRaises(ProtocolError),
                    ):
                        codec.decode(malformed)

        check(
            protocol.COMMANDS,
            (
                Get("k"),
                Delete("k"),
                TTL("k"),
                Persist("k"),
                Set("k", b"v"),
                Expire("k", 1),
                Subscribe("c"),
                Unsubscribe("c"),
                Publish("c", b"v"),
            ),
        )
        check(
            protocol.SERVER_MESSAGES,
            (
                Ok(),
                TTLValue(1 << 62),
                Persistent(),
                Nil(),
                Value(b"v"),
                Err("error"),
                Subscribed("c"),
                Unsubscribed("c"),
                Delivered(1),
                Message("c", b"v"),
            ),
        )

    async def test_command_roundtrips(self) -> None:
        for command in (
            Get("ქართული"),
            Delete("k"),
            TTL("k"),
            Persist("k"),
            Set("key", bytes(range(256))),
            Expire("key", (1 << 64) - 1),
            Subscribe("news"),
            Unsubscribe("news"),
            Publish("news", b"\x00\xff"),
        ):
            with self.subTest(command=command):
                request = command
                encoded = protocol.DEFAULT_PROTOCOL.framed_commands.encode(request)
                self.assertEqual(
                    await anext(
                        protocol.DEFAULT_PROTOCOL.read_commands(stream(encoded))
                    ),
                    request,
                )

    async def test_largest_request_fields_fit_outer_frame(self) -> None:
        key = "\U0010ffff" * 512
        channel = "\U0010ffff" * 256
        payload = b"x" * protocol.MAX_FIELD_BYTES
        for command in (Set(key, payload), Publish(channel, payload)):
            with self.subTest(command=type(command).__name__):
                request = command
                encoded = protocol.DEFAULT_PROTOCOL.framed_commands.encode(request)
                self.assertEqual(
                    await anext(
                        protocol.DEFAULT_PROTOCOL.read_commands(stream(encoded))
                    ),
                    request,
                )
        message = Message(channel, payload)
        encoded = protocol.DEFAULT_PROTOCOL.framed_messages.encode(message)
        self.assertEqual(
            await anext(protocol.DEFAULT_PROTOCOL.read_messages(stream(encoded))),
            message,
        )

    async def test_reply_and_message_roundtrips(self) -> None:
        for reply in (
            Ok(),
            TTLValue(1 << 62),
            Persistent(),
            Nil(),
            Value(b"\xff"),
            Err("bad command"),
            Subscribed("news"),
            Unsubscribed("news"),
            Delivered((1 << 32) - 1),
        ):
            with self.subTest(reply=reply):
                response = reply
                encoded = protocol.DEFAULT_PROTOCOL.framed_messages.encode(response)
                self.assertEqual(
                    await anext(
                        protocol.DEFAULT_PROTOCOL.read_messages(stream(encoded))
                    ),
                    response,
                )
        payload = b"x" * protocol.MAX_FIELD_BYTES
        self.assertEqual(
            await anext(
                protocol.DEFAULT_PROTOCOL.read_messages(
                    stream(
                        protocol.DEFAULT_PROTOCOL.framed_messages.encode(
                            Message("news", payload)
                        )
                    )
                )
            ),
            Message("news", payload),
        )

    async def test_encoders_reject_oversized_fields(self) -> None:
        big = b"x" * (protocol.MAX_FIELD_BYTES + 1)
        encoders = (
            lambda: protocol.DEFAULT_PROTOCOL.framed_commands.encode(Set("key", big)),
            lambda: protocol.DEFAULT_PROTOCOL.framed_messages.encode(Value(big)),
            lambda: protocol.DEFAULT_PROTOCOL.framed_messages.encode(
                Message("news", big)
            ),
            lambda: protocol.DEFAULT_PROTOCOL.framed_messages.encode(
                Message("x" * (protocol.MAX_FIELD_BYTES + 1), b"")
            ),
        )
        for encode in encoders:
            with self.assertRaisesRegex(ProtocolError, "field too long"):
                encode()

    async def test_encoders_reject_invalid_integer_ranges(self) -> None:
        for value in (-1, 1 << 64):
            with self.subTest(ttl=value), self.assertRaises(ProtocolError):
                protocol.DEFAULT_PROTOCOL.framed_commands.encode(Expire("key", value))
        for value in (-1, 1 << 32):
            with self.subTest(delivered=value), self.assertRaises(ProtocolError):
                protocol.DEFAULT_PROTOCOL.framed_messages.encode(Delivered(value))

    async def test_encoder_rejects_invalid_utf8(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "invalid utf-8"):
            protocol.DEFAULT_PROTOCOL.framed_commands.encode(Get("\ud800"))

    async def test_decoder_rejects_invalid_fields(self) -> None:
        frames = (
            struct.pack(">I", protocol.MAX_FIELD_BYTES + 1),
            b"\x00\x00\x00\x01\xff",
            b"\x00\x00\x00\x04NOPE",
        )
        for frame in frames:
            with self.subTest(frame=frame), self.assertRaises(ProtocolError):
                await anext(
                    protocol.DEFAULT_PROTOCOL.read_commands(
                        stream(protocol.DEFAULT_PROTOCOL.framing.encode(frame))
                    )
                )

    async def test_truncated_command_raises_incomplete_read(self) -> None:
        encoded = protocol.DEFAULT_PROTOCOL.framed_commands.encode(Set("key", b"value"))
        with self.assertRaises(asyncio.IncompleteReadError):
            await anext(protocol.DEFAULT_PROTOCOL.read_commands(stream(encoded[:-1])))

    async def test_streaming_readers_handle_clean_and_partial_eof(self) -> None:
        wire = protocol.DEFAULT_PROTOCOL

        async def check[T](
            read: Callable[[asyncio.StreamReader], AsyncGenerator[T]],
            chain: Codec[T, bytes],
            values: tuple[T, ...],
        ) -> None:
            with self.subTest(reader=read):
                encoded = b"".join(chain.encode(value) for value in values)
                self.assertEqual(
                    [value async for value in read(stream(encoded))], list(values)
                )
                self.assertEqual([value async for value in read(stream(b""))], [])
                for partial in (encoded[:1], encoded[:3], encoded[:-1]):
                    with (
                        self.subTest(partial=partial),
                        self.assertRaises(asyncio.IncompleteReadError),
                    ):
                        [value async for value in read(stream(partial))]

        await check(wire.read_commands, wire.framed_commands, (Get("a"), TTL("b")))
        await check(
            wire.read_messages, wire.framed_messages, (Ok(), Message("news", b"push"))
        )

    async def test_fragmented_and_consecutive_messages(self) -> None:
        first, second = Set("key", b"value"), Get("key")
        encoded = protocol.DEFAULT_PROTOCOL.framed_commands.encode(
            first
        ) + protocol.DEFAULT_PROTOCOL.framed_commands.encode(second)
        reader = asyncio.StreamReader()
        commands = protocol.DEFAULT_PROTOCOL.read_commands(reader)
        await self.enterAsyncContext(contextlib.aclosing(commands))
        task = asyncio.create_task(anext(commands))
        for byte in encoded:
            reader.feed_data(bytes([byte]))
            await asyncio.sleep(0)
        reader.feed_eof()
        self.assertEqual(await task, first)
        self.assertEqual(await anext(commands), second)
        with self.assertRaises(StopAsyncIteration):
            await anext(commands)

    async def test_oversized_outer_length_rejected_before_reading_body(self) -> None:
        self.assertEqual(protocol.MAX_FRAME_BODY_BYTES, 2 << 20)
        for read in (
            protocol.DEFAULT_PROTOCOL.read_commands,
            protocol.DEFAULT_PROTOCOL.read_messages,
        ):
            reader = asyncio.StreamReader()
            reader.feed_data(struct.pack(">I", protocol.MAX_FRAME_BODY_BYTES + 1))
            with (
                self.subTest(read=read.__name__),
                self.assertRaisesRegex(ProtocolError, "field too long"),
            ):
                async with asyncio.timeout(1):
                    await anext(read(reader))

    async def test_complete_frame_rejects_truncated_or_trailing_record(self) -> None:
        body = protocol.COMMANDS.encode(Set("key", b"value"))
        for malformed in (body[:-1], body + b"extra"):
            with self.subTest(body=malformed), self.assertRaises(ProtocolError):
                await anext(
                    protocol.DEFAULT_PROTOCOL.read_commands(
                        stream(protocol.DEFAULT_PROTOCOL.framing.encode(malformed))
                    )
                )

    def test_wire_bytes_are_explicitly_length_prefixed(self) -> None:
        self.assertEqual(
            protocol.DEFAULT_PROTOCOL.framed_commands.encode(Get("k")),
            b"\x00\x00\x00\x0c\x00\x00\x00\x03GET\x00\x00\x00\x01k",
        )
        self.assertEqual(
            protocol.DEFAULT_PROTOCOL.framed_messages.encode(Ok()),
            b"\x00\x00\x00\x06\x00\x00\x00\x02OK",
        )

    def test_sync_body_codecs_roundtrip_and_reject_wrong_message_direction(
        self,
    ) -> None:
        command = Set("key", b"value")
        self.assertEqual(
            protocol.COMMANDS.decode(protocol.COMMANDS.encode(command)),
            command,
        )
        self.assertEqual(
            protocol.SERVER_MESSAGES.decode(
                protocol.SERVER_MESSAGES.encode(Message("ch", b"v"))
            ),
            Message("ch", b"v"),
        )
        with self.assertRaisesRegex(ProtocolError, "unknown command"):
            protocol.COMMANDS.decode(protocol.SERVER_MESSAGES.encode(Ok()))
        with self.assertRaisesRegex(ProtocolError, "unknown server message"):
            protocol.SERVER_MESSAGES.decode(protocol.COMMANDS.encode(Get("key")))
