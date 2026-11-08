import asyncio
import random
import struct
import unittest

from velis.codec import Codec
from velis.core.commands import (
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
    Ttl,
    TtlValue,
    Unsubscribe,
    Unsubscribed,
    Value,
)
from velis.errors import ProtocolError
from velis.net import protocol
from velis.net.protocol import Request, Response


def stream(data: bytes) -> asyncio.StreamReader:
    """Expose an encoded frame as a complete in-memory stream."""
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
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
                # Preserve the outer tag/header while varying the remaining fields.
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
            Ttl("key"),
            Persist("key"),
            Set("key", b"\x00\xff"),
            Expire("key", 1),
            Subscribe("ch"),
            Unsubscribe("ch"),
            Publish("ch", b"hello"),
        )
        frames = (
            Ok(),
            TtlValue(1 << 62),
            Persistent(),
            Nil(),
            Value(b"\xff"),
            Err("error"),
            Subscribed("ch"),
            Unsubscribed("ch"),
            Delivered(42),
            Message("ch", b"hello"),
        )
        check(protocol.CommandCodec(), commands)
        check(protocol.FrameCodec(), frames)
        check(
            protocol.DEFAULT_PROTOCOL.commands,
            tuple(Request(i, command) for i, command in enumerate(commands, 1)),
        )
        check(
            protocol.DEFAULT_PROTOCOL.frames,
            (
                *(
                    frame if isinstance(frame, Message) else Response(i, frame)
                    for i, frame in enumerate(frames, 1)
                ),
                Response(0, Err("connection error")),
            ),
        )

    def test_command_names_enforce_character_limits_in_both_directions(self) -> None:
        codec = protocol.CommandCodec()
        for tag, make, limit, suffix in (
            ("GET", Get, 512, b""),
            ("DEL", Delete, 512, b""),
            ("TTL", Ttl, 512, b""),
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
                (struct.pack(">I", byte_limit + 1), "truncated record"),
                (protocol.BYTES.encode(b"\xff" * (byte_limit + 1)), "field too long"),
                (protocol.BYTES.encode(b"\xff" * byte_limit), "invalid utf-8"),
                (protocol.TEXT.encode("x" * (limit + 1)), f"{what} exceeds {limit}"),
                (protocol.TEXT.encode(""), f"{what} must not be empty"),
            ):
                with (
                    self.subTest(tag=tag, error=error),
                    self.assertRaisesRegex(ProtocolError, error),
                ):
                    protocol.CommandCodec().decode(protocol.TEXT.encode(tag) + name)

    def test_tags_reject_oversized_data_before_utf8_decoding(self) -> None:
        for codec in (protocol.CommandCodec(), protocol.FrameCodec()):
            for tag, error in (
                (b"\xff" * 33, "field too long"),
                (b"\xff" * 32, "invalid utf-8"),
                (b"x" * 32, "unknown"),
            ):
                with (
                    self.subTest(codec=codec.name, tag=tag),
                    self.assertRaisesRegex(ProtocolError, error),
                ):
                    codec.decode(protocol.BYTES.encode(tag))

    def test_reply_channels_keep_existing_text_rules(self) -> None:
        codec = protocol.FrameCodec()
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
            protocol.CommandCodec(),
            (
                Get("k"),
                Delete("k"),
                Ttl("k"),
                Persist("k"),
                Set("k", b"v"),
                Expire("k", 1),
                Subscribe("c"),
                Unsubscribe("c"),
                Publish("c", b"v"),
            ),
        )
        check(
            protocol.FrameCodec(),
            (
                Ok(),
                TtlValue(1 << 62),
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
            Ttl("k"),
            Persist("k"),
            Set("key", bytes(range(256))),
            Expire("key", (1 << 64) - 1),
            Subscribe("news"),
            Unsubscribe("news"),
            Publish("news", b"\x00\xff"),
        ):
            with self.subTest(command=command):
                request = Request(42, command)
                encoded = protocol.DEFAULT_PROTOCOL.commands.encode(request)
                self.assertEqual(
                    await protocol.DEFAULT_PROTOCOL.read_command(stream(encoded)),
                    request,
                )

    async def test_largest_request_fields_fit_outer_frame(self) -> None:
        key = "\U0010ffff" * 512
        channel = "\U0010ffff" * 256
        payload = b"x" * protocol.MAX_LEN
        for command in (Set(key, payload), Publish(channel, payload)):
            with self.subTest(command=type(command).__name__):
                request = Request(protocol.MAX_REQUEST_ID, command)
                encoded = protocol.DEFAULT_PROTOCOL.commands.encode(request)
                self.assertEqual(
                    await protocol.DEFAULT_PROTOCOL.read_command(stream(encoded)),
                    request,
                )
        message = Message(channel, payload)
        encoded = protocol.DEFAULT_PROTOCOL.frames.encode(message)
        self.assertEqual(
            await protocol.DEFAULT_PROTOCOL.read_frame(stream(encoded)), message
        )

    async def test_reply_and_message_roundtrips(self) -> None:
        for reply in (
            Ok(),
            TtlValue(1 << 62),
            Persistent(),
            Nil(),
            Value(b"\xff"),
            Err("bad command"),
            Subscribed("news"),
            Unsubscribed("news"),
            Delivered((1 << 32) - 1),
        ):
            with self.subTest(reply=reply):
                response = Response(42, reply)
                encoded = protocol.DEFAULT_PROTOCOL.frames.encode(response)
                self.assertEqual(
                    await protocol.DEFAULT_PROTOCOL.read_frame(stream(encoded)),
                    response,
                )
        payload = b"x" * protocol.MAX_LEN
        self.assertEqual(
            await protocol.DEFAULT_PROTOCOL.read_frame(
                stream(
                    protocol.DEFAULT_PROTOCOL.frames.encode(Message("news", payload))
                )
            ),
            Message("news", payload),
        )

    async def test_encoders_reject_oversized_fields(self) -> None:
        big = b"x" * (protocol.MAX_LEN + 1)
        encoders = (
            lambda: protocol.DEFAULT_PROTOCOL.commands.encode(
                Request(1, Set("key", big))
            ),
            lambda: protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Value(big))),
            lambda: protocol.DEFAULT_PROTOCOL.frames.encode(Message("news", big)),
            lambda: protocol.DEFAULT_PROTOCOL.frames.encode(
                Message("x" * (protocol.MAX_LEN + 1), b"")
            ),
        )
        for encode in encoders:
            with self.assertRaisesRegex(ProtocolError, "field too long"):
                encode()

    async def test_encoders_reject_invalid_integer_ranges(self) -> None:
        for value in (-1, 1 << 64):
            with self.subTest(ttl=value), self.assertRaises(ProtocolError):
                protocol.DEFAULT_PROTOCOL.commands.encode(
                    Request(1, Expire("key", value))
                )
        for value in (-1, 1 << 32):
            with self.subTest(delivered=value), self.assertRaises(ProtocolError):
                protocol.DEFAULT_PROTOCOL.frames.encode(Response(1, Delivered(value)))

    async def test_encoder_rejects_invalid_utf8(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "invalid utf-8"):
            protocol.DEFAULT_PROTOCOL.commands.encode(Request(1, Get("\ud800")))

    async def test_decoder_rejects_invalid_fields(self) -> None:
        frames = (
            struct.pack(">I", protocol.MAX_LEN + 1),
            b"\x00\x00\x00\x01\xff",
            b"\x00\x00\x00\x04NOPE",
        )
        for frame in frames:
            with self.subTest(frame=frame), self.assertRaises(ProtocolError):
                await protocol.DEFAULT_PROTOCOL.read_command(
                    stream(
                        protocol.DEFAULT_PROTOCOL.framing.encode(
                            struct.pack(">Q", 1) + frame
                        )
                    )
                )

    async def test_truncated_command_raises_incomplete_read(self) -> None:
        encoded = protocol.DEFAULT_PROTOCOL.commands.encode(
            Request(1, Set("key", b"value"))
        )
        with self.assertRaises(asyncio.IncompleteReadError):
            await protocol.DEFAULT_PROTOCOL.read_command(stream(encoded[:-1]))

    async def test_fragmented_and_consecutive_messages(self) -> None:
        first, second = Request(1, Set("key", b"value")), Request(2, Get("key"))
        encoded = protocol.DEFAULT_PROTOCOL.commands.encode(
            first
        ) + protocol.DEFAULT_PROTOCOL.commands.encode(second)
        reader = asyncio.StreamReader()
        task = asyncio.create_task(protocol.DEFAULT_PROTOCOL.read_command(reader))
        for byte in encoded:
            reader.feed_data(bytes([byte]))
            await asyncio.sleep(0)
        reader.feed_eof()
        self.assertEqual(await task, first)
        self.assertEqual(await protocol.DEFAULT_PROTOCOL.read_command(reader), second)

    async def test_oversized_outer_length_rejected_before_reading_body(self) -> None:
        self.assertEqual(protocol.MAX_FRAME_LEN, 2 << 20)
        for read in (
            protocol.DEFAULT_PROTOCOL.read_command,
            protocol.DEFAULT_PROTOCOL.read_frame,
        ):
            reader = asyncio.StreamReader()
            reader.feed_data(struct.pack(">I", protocol.MAX_FRAME_LEN + 1))
            # No EOF or body: rejecting the header must not wait for either.
            with (
                self.subTest(read=read.__name__),
                self.assertRaisesRegex(ProtocolError, "field too long"),
            ):
                async with asyncio.timeout(1):
                    await read(reader)

    async def test_complete_frame_rejects_truncated_or_trailing_record(self) -> None:
        body = struct.pack(">Q", 1) + protocol.CommandCodec().encode(
            Set("key", b"value")
        )
        for malformed in (body[:-1], body + b"extra"):
            with self.subTest(body=malformed), self.assertRaises(ProtocolError):
                await protocol.DEFAULT_PROTOCOL.read_command(
                    stream(protocol.DEFAULT_PROTOCOL.framing.encode(malformed))
                )

    def test_wire_bytes_are_explicitly_length_prefixed(self) -> None:
        self.assertEqual(
            protocol.DEFAULT_PROTOCOL.commands.encode(Request(7, Get("k"))),
            b"\x00\x00\x00\x14"
            + struct.pack(">Q", 7)
            + b"\x00\x00\x00\x03GET\x00\x00\x00\x01k",
        )
        self.assertEqual(
            protocol.DEFAULT_PROTOCOL.frames.encode(Response(7, Ok())),
            b"\x00\x00\x00\x0e" + struct.pack(">Q", 7) + b"\x00\x00\x00\x02OK",
        )

    def test_request_id_bounds_and_reserved_zero(self) -> None:
        wire = protocol.DEFAULT_PROTOCOL
        for request_id in (1, protocol.MAX_REQUEST_ID):
            request = Request(request_id, Get("key"))
            response = Response(request_id, Value(b"value"))
            self.assertEqual(
                wire.commands.decode(wire.commands.encode(request)), request
            )
            self.assertEqual(wire.frames.decode(wire.frames.encode(response)), response)
        for request_id in (-1, 0, protocol.MAX_REQUEST_ID + 1):
            with self.subTest(request_id=request_id), self.assertRaises(ProtocolError):
                wire.commands.encode(Request(request_id, Get("key")))
        for request_id in (-1, protocol.MAX_REQUEST_ID + 1):
            with self.subTest(request_id=request_id), self.assertRaises(ProtocolError):
                wire.frames.encode(Response(request_id, Ok()))
        error = Response(0, Err("connection limit exceeded"))
        self.assertEqual(wire.frames.decode(wire.frames.encode(error)), error)
        with self.assertRaisesRegex(ProtocolError, "nonzero"):
            wire.commands.decode(
                wire.framing.encode(
                    struct.pack(">Q", 0) + protocol.CommandCodec().encode(Get("key"))
                )
            )
        with self.assertRaisesRegex(ProtocolError, "only connection errors"):
            wire.frames.encode(Response(0, Ok()))
        with self.assertRaisesRegex(ProtocolError, "only connection errors"):
            wire.frames.decode(
                wire.framing.encode(
                    struct.pack(">Q", 0) + protocol.FrameCodec().encode(Ok())
                )
            )
        with self.assertRaisesRegex(ProtocolError, "pushes must use request ID zero"):
            wire.frames.decode(
                wire.framing.encode(
                    struct.pack(">Q", 1)
                    + protocol.FrameCodec().encode(Message("ch", b"x"))
                )
            )

    async def test_truncated_id_is_a_protocol_error_in_a_complete_frame(self) -> None:
        wire = protocol.DEFAULT_PROTOCOL
        for length in range(8):
            for read in (wire.read_command, wire.read_frame):
                with (
                    self.subTest(length=length, read=read.__name__),
                    self.assertRaises(ProtocolError),
                ):
                    await read(stream(wire.framing.encode(b"\x00" * length)))

    def test_sync_body_codecs_roundtrip_and_reject_wrong_message_direction(
        self,
    ) -> None:
        command = Set("key", b"value")
        self.assertEqual(
            protocol.CommandCodec().decode(protocol.CommandCodec().encode(command)),
            command,
        )
        self.assertEqual(
            protocol.FrameCodec().decode(
                protocol.FrameCodec().encode(Message("ch", b"v"))
            ),
            Message("ch", b"v"),
        )
        with self.assertRaisesRegex(ProtocolError, "unknown command"):
            protocol.CommandCodec().decode(protocol.FrameCodec().encode(Ok()))
        with self.assertRaisesRegex(ProtocolError, "unknown frame"):
            protocol.FrameCodec().decode(protocol.CommandCodec().encode(Get("key")))
