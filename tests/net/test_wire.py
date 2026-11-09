import random
import struct
import unittest
from collections.abc import Callable
from typing import TypeAliasType, cast, get_args

from velis.core.messages import (
    TTL,
    Command,
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
from velis.net.limits import (
    MAX_CHANNEL_CHARS,
    MAX_FIELD_BYTES,
    MAX_KEY_CHARS,
    MAX_TAG_BYTES,
)
from velis.net.protocol import COMMANDS, SERVER_MESSAGES, ServerFrame
from velis.net.wire import Codec
from velis.net.wire import text as text_field

COMMAND_CASES: tuple[tuple[Command, bytes], ...] = (
    (Get("k"), b"\x00\x00\x00\x03GET\x00\x00\x00\x01k"),
    (Get("é"), b"\x00\x00\x00\x03GET\x00\x00\x00\x02\xc3\xa9"),
    (
        Set("k", b"\x00\xff"),
        b"\x00\x00\x00\x03SET\x00\x00\x00\x01k\x00\x00\x00\x02\x00\xff",
    ),
    (
        Expire("k", 0x0102030405060708),
        b"\x00\x00\x00\x06EXPIRE\x00\x00\x00\x01k\x01\x02\x03\x04\x05\x06\x07\x08",
    ),
    (Delete("k"), b"\x00\x00\x00\x03DEL\x00\x00\x00\x01k"),
    (TTL("k"), b"\x00\x00\x00\x03TTL\x00\x00\x00\x01k"),
    (Persist("k"), b"\x00\x00\x00\x07PERSIST\x00\x00\x00\x01k"),
    (Subscribe("c"), b"\x00\x00\x00\x09SUBSCRIBE\x00\x00\x00\x01c"),
    (Unsubscribe("c"), b"\x00\x00\x00\x0bUNSUBSCRIBE\x00\x00\x00\x01c"),
    (Publish("c", b"p"), b"\x00\x00\x00\x07PUBLISH\x00\x00\x00\x01c\x00\x00\x00\x01p"),
)
MESSAGE_CASES: tuple[tuple[ServerFrame, bytes], ...] = (
    (Ok(), b"\x00\x00\x00\x02OK"),
    (Nil(), b"\x00\x00\x00\x03NIL"),
    (Persistent(), b"\x00\x00\x00\x0aPERSISTENT"),
    (Value(b"\xff"), b"\x00\x00\x00\x05VALUE\x00\x00\x00\x01\xff"),
    (Err("bad"), b"\x00\x00\x00\x03ERR\x00\x00\x00\x03bad"),
    (Subscribed("c"), b"\x00\x00\x00\x0aSUBSCRIBED\x00\x00\x00\x01c"),
    (Unsubscribed("c"), b"\x00\x00\x00\x0cUNSUBSCRIBED\x00\x00\x00\x01c"),
    (Delivered(0x01020304), b"\x00\x00\x00\x09DELIVERED\x01\x02\x03\x04"),
    (
        TTLValue(0x0102030405060708),
        b"\x00\x00\x00\x09TTL_VALUE\x01\x02\x03\x04\x05\x06\x07\x08",
    ),
    (Message("c", b"p"), b"\x00\x00\x00\x07MESSAGE\x00\x00\x00\x01c\x00\x00\x00\x01p"),
)


def field(data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + data


def tag(value: str) -> bytes:
    return field(value.encode())


def members(alias: TypeAliasType) -> set[type]:
    result: set[type] = set()
    for variant in get_args(alias.__value__):
        result |= members(variant) if isinstance(variant, TypeAliasType) else {variant}
    return result


class WireTests(unittest.TestCase):
    def test_duplicate_tags_and_mismatched_field_counts_fail_at_declaration(
        self,
    ) -> None:
        key = text_field("key", MAX_KEY_CHARS)
        with self.assertRaisesRegex(ValueError, "duplicate command tag"):
            Codec[Command]("command", {Get: (b"DUP", key), Delete: (b"DUP", key)})
        for schema in ((b"GET",), (b"GET", key, key)):
            with (
                self.subTest(schema=schema),
                self.assertRaisesRegex(TypeError, "Get has 1 attributes"),
            ):
                Codec[Command]("command", {Get: schema})

    def test_unregistered_subclasses_are_rejected(self) -> None:
        class DerivedGet(Get):
            pass

        class DerivedValue(Value):
            pass

        with self.assertRaisesRegex(ProtocolError, "unsupported command: DerivedGet"):
            COMMANDS.encode(DerivedGet("key"))
        with self.assertRaisesRegex(
            ProtocolError, "unsupported server message: DerivedValue"
        ):
            SERVER_MESSAGES.encode(DerivedValue(b"value"))

    def _check_golden[T](
        self,
        encode: Callable[[T], bytes],
        decode: Callable[[bytes], T],
        cases: tuple[tuple[T, bytes], ...],
    ) -> None:
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(encode(value), expected)
                self.assertEqual(decode(expected), value)

    def test_golden_bodies_cover_every_command_reply_and_push(self) -> None:
        self.assertEqual({type(value) for value, _ in COMMAND_CASES}, members(Command))
        self.assertEqual(
            {type(value) for value, _ in MESSAGE_CASES}, members(ServerFrame)
        )
        self._check_golden(COMMANDS.encode, COMMANDS.decode, COMMAND_CASES)
        self._check_golden(
            SERVER_MESSAGES.encode,
            SERVER_MESSAGES.decode,
            MESSAGE_CASES,
        )

    def test_every_truncation_and_trailing_byte_is_rejected(self) -> None:
        for codec, cases in (
            (COMMANDS, COMMAND_CASES),
            (SERVER_MESSAGES, MESSAGE_CASES),
        ):
            for value, body in cases:
                for malformed in (
                    *[body[:end] for end in range(len(body))],
                    body + b"Z",
                ):
                    with (
                        self.subTest(variant=type(value), body=malformed),
                        self.assertRaises(ProtocolError),
                    ):
                        codec.decode(malformed)

    def test_mutations_and_arbitrary_bytes_only_raise_protocol_errors(self) -> None:
        def check[T](
            encode: Callable[[T], bytes],
            decode: Callable[[bytes], T],
            cases: tuple[tuple[T, bytes], ...],
        ) -> None:
            rng = random.Random(0)
            seeds = [body for _, body in cases]
            bodies = [rng.randbytes(rng.randrange(256)) for _ in range(2000)]
            for _ in range(20000):
                body = bytearray(rng.choice(seeds))
                body[rng.randrange(len(body))] = rng.randrange(256)
                bodies.append(bytes(body))
            for body in bodies:
                try:
                    value = decode(body)
                except ProtocolError:
                    continue
                self.assertEqual(encode(value), body)

        check(COMMANDS.encode, COMMANDS.decode, COMMAND_CASES)
        check(
            SERVER_MESSAGES.encode,
            SERVER_MESSAGES.decode,
            MESSAGE_CASES,
        )

    def test_exact_errors_and_validation_precedence(self) -> None:
        for codec, kind in (
            (COMMANDS, "command"),
            (SERVER_MESSAGES, "server message"),
        ):
            for body, expected in (
                (b"", "truncated record: need 4 bytes"),
                (
                    struct.pack(">I", MAX_TAG_BYTES + 1),
                    f"field too long: {MAX_TAG_BYTES + 1}",
                ),
                (field(b"\xff"), "invalid utf-8 in string field"),
                (field(b""), f"unknown {kind}: ''"),
                (tag("NOPE") + b"unread", f"unknown {kind}: 'NOPE'"),
            ):
                with (
                    self.subTest(kind=kind, body=body),
                    self.assertRaises(ProtocolError) as raised,
                ):
                    codec.decode(body)
                self.assertEqual(str(raised.exception), expected)
        for body, expected in (
            (tag("GET") + field(b""), "key must not be empty"),
            (tag("GET") + field(b"\xff"), "invalid utf-8 in string field"),
            (
                tag("GET") + struct.pack(">I", MAX_KEY_CHARS * 4 + 1),
                f"field too long: {MAX_KEY_CHARS * 4 + 1}",
            ),
            (
                tag("GET") + field(b"x" * (MAX_KEY_CHARS + 1)),
                f"key exceeds {MAX_KEY_CHARS} characters",
            ),
            (
                tag("EXPIRE") + field(b"k") + b"\x00" * 7,
                "truncated record: need 8 bytes",
            ),
            (
                tag("SET") + field(b"k") + struct.pack(">I", 3) + b"ab",
                "truncated record: need 3 bytes",
            ),
            (tag("GET") + field(b"k") + b"Z", "trailing bytes after record"),
        ):
            with self.subTest(body=body), self.assertRaises(ProtocolError) as raised:
                COMMANDS.decode(body)
            self.assertEqual(str(raised.exception), expected)
        for body, expected in (
            (tag("DELIVERED") + b"\x00" * 3, "truncated record: need 4 bytes"),
            (tag("TTL_VALUE") + b"\x00" * 7, "truncated record: need 8 bytes"),
            (tag("ERR") + field(b"\xff"), "invalid utf-8 in string field"),
            (tag("OK") + b"Z", "trailing bytes after record"),
            (
                tag("MESSAGE") + field(b"") + struct.pack(">I", MAX_FIELD_BYTES + 1),
                f"field too long: {MAX_FIELD_BYTES + 1}",
            ),
        ):
            with self.subTest(body=body), self.assertRaises(ProtocolError) as raised:
                SERVER_MESSAGES.decode(body)
            self.assertEqual(str(raised.exception), expected)

    def test_request_text_boundaries_in_both_directions(self) -> None:
        for name, cls, what, limit, suffix in (
            ("GET", Get, "key", MAX_KEY_CHARS, b""),
            ("DEL", Delete, "key", MAX_KEY_CHARS, b""),
            ("TTL", TTL, "key", MAX_KEY_CHARS, b""),
            ("PERSIST", Persist, "key", MAX_KEY_CHARS, b""),
            ("SET", lambda s: Set(s, b""), "key", MAX_KEY_CHARS, field(b"")),
            ("EXPIRE", lambda s: Expire(s, 0), "key", MAX_KEY_CHARS, b"\x00" * 8),
            ("SUBSCRIBE", Subscribe, "channel", MAX_CHANNEL_CHARS, b""),
            ("UNSUBSCRIBE", Unsubscribe, "channel", MAX_CHANNEL_CHARS, b""),
            (
                "PUBLISH",
                lambda s: Publish(s, b""),
                "channel",
                MAX_CHANNEL_CHARS,
                field(b""),
            ),
        ):
            for text in ("é" * limit, "\U0010ffff" * limit, " \x00\n"):
                value = cls(text)
                expected = tag(name) + field(text.encode()) + suffix
                with self.subTest(name=name, length=len(text)):
                    self.assertEqual(COMMANDS.encode(value), expected)
                    self.assertEqual(COMMANDS.decode(expected), value)
            for raw, expected_error in (
                (b"", f"{what} must not be empty"),
                (b"x" * (limit + 1), f"{what} exceeds {limit} characters"),
                (b"\xff" * (limit * 4), "invalid utf-8 in string field"),
                (b"\xff" * (limit * 4 + 1), f"field too long: {limit * 4 + 1}"),
            ):
                with (
                    self.subTest(name=name, length=len(raw)),
                    self.assertRaises(ProtocolError) as raised,
                ):
                    COMMANDS.decode(tag(name) + field(raw) + suffix)
                self.assertEqual(str(raised.exception), expected_error)

    def test_encoders_reject_invalid_fields_without_changing_errors(self) -> None:
        def check[T](
            encode: Callable[[T], bytes], cases: tuple[tuple[T, str], ...]
        ) -> None:
            for value, expected in cases:
                with (
                    self.subTest(variant=type(value)),
                    self.assertRaises(ProtocolError) as raised,
                ):
                    encode(value)
                self.assertEqual(str(raised.exception), expected)

        check(
            COMMANDS.encode,
            (
                (Get(""), "key must not be empty"),
                (
                    Get("x" * (MAX_KEY_CHARS + 1)),
                    f"key exceeds {MAX_KEY_CHARS} characters",
                ),
                (Get("\ud800"), "invalid utf-8 in string field"),
                (Subscribe(""), "channel must not be empty"),
                (
                    Subscribe("x" * (MAX_CHANNEL_CHARS + 1)),
                    f"channel exceeds {MAX_CHANNEL_CHARS} characters",
                ),
                (Expire("k", -1), "u64 integer out of range: -1"),
                (Expire("k", 1 << 64), f"u64 integer out of range: {1 << 64}"),
            ),
        )
        check(
            SERVER_MESSAGES.encode,
            (
                (Delivered(-1), "u32 integer out of range: -1"),
                (Delivered(1 << 32), f"u32 integer out of range: {1 << 32}"),
                (TTLValue(1 << 64), f"u64 integer out of range: {1 << 64}"),
                (Err("\ud800"), "invalid utf-8 in string field"),
            ),
        )

    def test_unsigned_integer_boundaries_roundtrip(self) -> None:
        for value in (0, (1 << 64) - 1):
            command = Expire("k", value)
            self.assertEqual(COMMANDS.decode(COMMANDS.encode(command)), command)
            reply = TTLValue(value)
            self.assertEqual(
                SERVER_MESSAGES.decode(SERVER_MESSAGES.encode(reply)),
                reply,
            )
        for value in (0, (1 << 32) - 1):
            reply = Delivered(value)
            self.assertEqual(
                SERVER_MESSAGES.decode(SERVER_MESSAGES.encode(reply)),
                reply,
            )

    def test_reply_channels_allow_empty_and_longer_text(self) -> None:
        for text in ("", "x" * (MAX_CHANNEL_CHARS + 1)):
            for value in (Message(text, b""), Subscribed(text), Unsubscribed(text)):
                self.assertEqual(
                    SERVER_MESSAGES.decode(SERVER_MESSAGES.encode(value)),
                    value,
                )

    def test_wrong_message_direction_is_rejected(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "unknown command"):
            COMMANDS.decode(SERVER_MESSAGES.encode(Ok()))
        with self.assertRaisesRegex(ProtocolError, "unknown server message"):
            SERVER_MESSAGES.decode(COMMANDS.encode(Get("k")))
        with self.assertRaisesRegex(ProtocolError, "unsupported command: Ok"):
            COMMANDS.encode(cast(Command, Ok()))
        with self.assertRaisesRegex(ProtocolError, "unsupported server message: Get"):
            SERVER_MESSAGES.encode(cast(ServerFrame, Get("k")))

    def test_payload_byte_limits_in_both_body_directions(self) -> None:
        data = b"x" * MAX_FIELD_BYTES
        for command in (Set("key", data), Publish("channel", data)):
            with self.subTest(variant=type(command)):
                self.assertEqual(COMMANDS.decode(COMMANDS.encode(command)), command)
        for message in (
            Value(data),
            Message("channel", data),
            Err("x" * MAX_FIELD_BYTES),
        ):
            with self.subTest(variant=type(message)):
                self.assertEqual(
                    SERVER_MESSAGES.decode(SERVER_MESSAGES.encode(message)), message
                )
        for command in (Set("key", data + b"x"), Publish("channel", data + b"x")):
            with self.subTest(variant=type(command)), self.assertRaises(ProtocolError):
                COMMANDS.encode(command)
        for message in (
            Value(data + b"x"),
            Message("channel", data + b"x"),
            Err("x" * (MAX_FIELD_BYTES + 1)),
        ):
            with self.subTest(variant=type(message)), self.assertRaises(ProtocolError):
                SERVER_MESSAGES.encode(message)
