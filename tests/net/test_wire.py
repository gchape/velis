import struct
import unittest
from dataclasses import dataclass
from typing import cast

from hypothesis import given
from hypothesis import strategies as st

from velis.core.messages import (
    Command,
    Err,
    Exists,
    Found,
    Keep,
    KeyPage,
    Lifetime,
    ListKeys,
    Message,
    Nil,
    Ok,
    Ping,
    Pong,
    Read,
    Remaining,
    RemainingValue,
    Remove,
    Send,
    Sent,
    Size,
    SizeValue,
    Unlimited,
    Unwatch,
    Unwatched,
    Value,
    Watch,
    Watching,
    Write,
)
from velis.errors import ProtocolError
from velis.net.limits import (
    MAX_CHANNEL_BYTES,
    MAX_FIELD_BYTES,
    MAX_KEY_CHARS,
    MAX_TAG_BYTES,
)
from velis.net.protocol import COMMANDS, SERVER_MESSAGES
from velis.net.wire import BLOB, U32, U64, Codec, Field, blob, free_text, msg, text


def prefixed(raw: bytes) -> bytes:
    return struct.pack(">I", len(raw)) + raw


COMMAND_FIXTURES = (
    (Ping(), b"\x00\x00\x00\x04PING"),
    (Exists("k"), b"\x00\x00\x00\x06EXISTS\x00\x00\x00\x01k"),
    (Size("k"), b"\x00\x00\x00\x04SIZE\x00\x00\x00\x01k"),
    (ListKeys("", 0, 2), b"\x00\x00\x00\x04LIST" + b"\x00" * 12 + b"\x00\x00\x00\x02"),
    (Read("k"), b"\x00\x00\x00\x04READ\x00\x00\x00\x01k"),
    (
        Write("k", b"\x00\xff"),
        b"\x00\x00\x00\x05WRITE\x00\x00\x00\x01k\x00\x00\x00\x02\x00\xff",
    ),
    (
        Lifetime("k", 258),
        b"\x00\x00\x00\x08LIFETIME\x00\x00\x00\x01k\x00\x00\x00\x00\x00\x00\x01\x02",
    ),
    (Remove("k"), b"\x00\x00\x00\x06REMOVE\x00\x00\x00\x01k"),
    (Remaining("k"), b"\x00\x00\x00\x09REMAINING\x00\x00\x00\x01k"),
    (Keep("k"), b"\x00\x00\x00\x04KEEP\x00\x00\x00\x01k"),
    (Watch("c"), b"\x00\x00\x00\x05WATCH\x00\x00\x00\x01c"),
    (Unwatch("c"), b"\x00\x00\x00\x07UNWATCH\x00\x00\x00\x01c"),
    (
        Send("c", b"\xff"),
        b"\x00\x00\x00\x04SEND\x00\x00\x00\x01c\x00\x00\x00\x01\xff",
    ),
)
SERVER_FIXTURES = (
    (Pong(), b"\x00\x00\x00\x04PONG"),
    (Found(), b"\x00\x00\x00\x05FOUND"),
    (SizeValue(258), b"\x00\x00\x00\x0aSIZE_VALUE\x00\x00\x00\x00\x00\x00\x01\x02"),
    (
        KeyPage(0, ("k",)),
        b"\x00\x00\x00\x08KEY_PAGE"
        + b"\x00" * 8
        + b"\x00\x00\x00\x01\x00\x00\x00\x01k",
    ),
    (Ok(), b"\x00\x00\x00\x02OK"),
    (Nil(), b"\x00\x00\x00\x03NIL"),
    (Unlimited(), b"\x00\x00\x00\x09UNLIMITED"),
    (Value(b"\xff"), b"\x00\x00\x00\x05VALUE\x00\x00\x00\x01\xff"),
    (Err("bad"), b"\x00\x00\x00\x03ERR\x00\x00\x00\x03bad"),
    (Watching("c"), b"\x00\x00\x00\x08WATCHING\x00\x00\x00\x01c"),
    (Unwatched("c"), b"\x00\x00\x00\x09UNWATCHED\x00\x00\x00\x01c"),
    (Sent(258), b"\x00\x00\x00\x04SENT\x00\x00\x01\x02"),
    (
        RemainingValue(258),
        b"\x00\x00\x00\x0fREMAINING_VALUE\x00\x00\x00\x00\x00\x00\x01\x02",
    ),
    (
        Message("c", b"\x00"),
        b"\x00\x00\x00\x07MESSAGE\x00\x00\x00\x01c\x00\x00\x00\x01\x00",
    ),
)


class WireTests(unittest.TestCase):
    def test_legacy_command_tags_are_rejected(self) -> None:
        """Only the current command vocabulary is accepted on the wire."""
        for tag in (
            b"GET",
            b"SET",
            b"EXPIRE",
            b"DEL",
            b"TTL",
            b"PERSIST",
            b"SUBSCRIBE",
            b"UNSUBSCRIBE",
            b"PUBLISH",
        ):
            with (
                self.subTest(tag=tag),
                self.assertRaisesRegex(ProtocolError, "unknown command"),
            ):
                COMMANDS.decode(prefixed(tag))

    def test_legacy_reply_tags_are_rejected(self) -> None:
        """Reply names follow the current command vocabulary exclusively."""
        for tag in (
            b"SUBSCRIBED",
            b"UNSUBSCRIBED",
            b"DELIVERED",
            b"TTL_VALUE",
            b"PERSISTENT",
        ):
            with (
                self.subTest(tag=tag),
                self.assertRaisesRegex(ProtocolError, "unknown server message"),
            ):
                SERVER_MESSAGES.decode(prefixed(tag))

    def test_noninteger_fields_raise_protocol_errors_before_packing(self) -> None:
        for field in (U32, U64):
            for value in (1.5, True, "2", float("nan"), float("inf")):
                with self.subTest(value=value), self.assertRaises(ProtocolError):
                    field.write(cast(int, value))

    def assert_fixtures[M](
        self, codec: Codec[M], fixtures: tuple[tuple[M, bytes], ...]
    ) -> None:
        for value, body in fixtures:
            with self.subTest(value=value):
                self.assertEqual(codec.encode(value), body)
                self.assertEqual(codec.decode(body), value)

    def test_golden_bytes_cover_every_command_reply_and_push(self) -> None:
        self.assert_fixtures(COMMANDS, COMMAND_FIXTURES)
        self.assert_fixtures(SERVER_MESSAGES, SERVER_FIXTURES)

    def test_decode_buffer_span_cannot_read_from_the_next_frame(self) -> None:
        for codec, fixtures in (
            (COMMANDS, COMMAND_FIXTURES),
            (SERVER_MESSAGES, SERVER_FIXTURES),
        ):
            for value, body in fixtures:
                for data in (
                    b"prefix" + body + b"suffix",
                    bytearray(b"prefix" + body + b"suffix"),
                ):
                    with self.subTest(value=value, buffer=type(data)):
                        self.assertEqual(codec.decode(data, 6, 6 + len(body)), value)
                        for end in range(len(body)):
                            with self.assertRaises(ProtocolError):
                                codec.decode(data, 6, 6 + end)
                        with self.assertRaisesRegex(ProtocolError, "trailing bytes"):
                            codec.decode(data, 6, len(data))

    def test_decoded_value_owns_its_bytes_and_does_not_pin_input(self) -> None:
        body = bytearray(COMMANDS.encode(Write("key", b"original")))
        command = COMMANDS.decode(body)
        body[:] = b"changed"
        body.clear()
        self.assertEqual(command, Write("key", b"original"))
        self.assertIsInstance(command, Write)
        self.assertIs(type(cast(Write, command).value), bytes)

    def test_invalid_record_bounds_raise_protocol_errors(self) -> None:
        body = COMMANDS.encode(Read("key"))
        for start, end in ((-1, 0), (2, 1), (0, len(body) + 1)):
            with self.subTest(start=start, end=end), self.assertRaises(ProtocolError):
                COMMANDS.decode(body, start, end)

    def test_every_truncation_and_trailing_data_is_rejected(self) -> None:
        for codec, fixtures in (
            (COMMANDS, COMMAND_FIXTURES),
            (SERVER_MESSAGES, SERVER_FIXTURES),
        ):
            for value, body in fixtures:
                for end in range(len(body)):
                    with (
                        self.subTest(value=value, end=end),
                        self.assertRaises(ProtocolError),
                    ):
                        codec.decode(body[:end])
                with (
                    self.subTest(value=value),
                    self.assertRaisesRegex(ProtocolError, "trailing bytes"),
                ):
                    codec.decode(body + b"\x00")

    def test_unknown_and_invalid_utf8_tags_fail_with_protocol_errors(self) -> None:
        for codec in (COMMANDS, SERVER_MESSAGES):
            for tag in (b"", b"UNKNOWN", b"\xff"):
                with self.subTest(tag=tag), self.assertRaises(ProtocolError):
                    codec.decode(prefixed(tag))
            with self.assertRaisesRegex(ProtocolError, "field too long"):
                codec.decode(struct.pack(">I", MAX_TAG_BYTES + 1))

    def test_key_limits_count_characters_and_accept_four_byte_utf8(self) -> None:
        key = "😀" * MAX_KEY_CHARS
        self.assertEqual(COMMANDS.decode(COMMANDS.encode(Read(key))), Read(key))
        for invalid in ("", "a" * (MAX_KEY_CHARS + 1), "\ud800"):
            with self.subTest(invalid=repr(invalid)), self.assertRaises(ProtocolError):
                COMMANDS.encode(Read(invalid))
        for raw in (b"", b"a" * (MAX_KEY_CHARS + 1), b"\xff"):
            with self.subTest(raw=raw), self.assertRaises(ProtocolError):
                COMMANDS.decode(prefixed(b"READ") + prefixed(raw))

    def test_echoed_channels_allow_empty_text_but_obey_encoded_byte_limit(self) -> None:
        for cls in (Watching, Unwatched):
            for channel in (
                "",
                "a" * MAX_CHANNEL_BYTES,
                "😀" * (MAX_CHANNEL_BYTES // 4),
            ):
                value = cls(channel)
                self.assertEqual(
                    SERVER_MESSAGES.decode(SERVER_MESSAGES.encode(value)), value
                )
            with self.assertRaises(ProtocolError):
                SERVER_MESSAGES.encode(cls("a" * (MAX_CHANNEL_BYTES + 1)))
        for raw in (b"\xff", b"a" * (MAX_CHANNEL_BYTES + 1)):
            with self.assertRaises(ProtocolError):
                SERVER_MESSAGES.decode(
                    prefixed(b"MESSAGE") + prefixed(raw) + prefixed(b"")
                )
        with self.assertRaises(ProtocolError):
            COMMANDS.encode(Watch(""))

    def test_blob_accepts_exact_limit_and_rejects_oversized_header_before_body(
        self,
    ) -> None:
        raw = b"\xff" * MAX_FIELD_BYTES
        self.assertEqual(
            BLOB.read(BLOB.write(raw), 0, 4 + len(raw)), (raw, 4 + len(raw))
        )
        with self.assertRaisesRegex(ProtocolError, "field too long"):
            BLOB.write(raw + b"x")
        with self.assertRaisesRegex(ProtocolError, "field too long"):
            BLOB.read(struct.pack(">I", MAX_FIELD_BYTES + 1), 0, 4)

    def assert_field_bounds[T](self, field: Field[T], value: T) -> None:
        encoded = field.write(value)
        data = b"prefix" + encoded + b"suffix"
        stop = 6 + len(encoded)
        self.assertEqual(field.read(data, 6, stop), (value, stop))
        with self.assertRaisesRegex(ProtocolError, "truncated"):
            field.read(data, 6, stop - 1)

    def test_field_readers_obey_offsets_and_end_boundary(self) -> None:
        self.assert_field_bounds(blob(3), b"abc")
        self.assert_field_bounds(free_text(4), "é")
        self.assert_field_bounds(text("key", 2), "😀")
        self.assert_field_bounds(U32, 123)
        self.assert_field_bounds(U64, 456)

    def test_free_text_rejects_invalid_utf8_in_both_directions(self) -> None:
        field = free_text(10)
        self.assertEqual(field.write(""), b"\x00" * 4)
        with self.assertRaisesRegex(ProtocolError, "invalid utf-8"):
            field.write("\ud800")
        with self.assertRaisesRegex(ProtocolError, "invalid utf-8"):
            field.read(prefixed(b"\xff"), 0, 5)
        with self.assertRaisesRegex(ProtocolError, "field too long"):
            free_text(3).write("😀")

    def test_unsigned_integer_boundaries(self) -> None:
        for field, width in ((U32, 4), (U64, 8)):
            for value in (0, 1, (1 << (width * 8)) - 1):
                encoded = value.to_bytes(width, "big")
                self.assertEqual(field.write(value), encoded)
                self.assertEqual(field.read(encoded, 0, width), (value, width))
            for value in (-1, 1 << (width * 8)):
                with self.assertRaisesRegex(ProtocolError, "out of range"):
                    field.write(value)

    @given(st.text(min_size=1, max_size=30), st.binary(max_size=200))
    def test_unicode_keys_and_opaque_values_roundtrip(
        self, key: str, value: bytes
    ) -> None:
        command = Write(key, value)
        self.assertEqual(COMMANDS.decode(COMMANDS.encode(command)), command)
        push = Message(key, value)
        self.assertEqual(SERVER_MESSAGES.decode(SERVER_MESSAGES.encode(push)), push)

    def assert_decode_contract[M](self, codec: Codec[M], data: bytes) -> None:
        try:
            decoded = codec.decode(data)
        except ProtocolError:
            return
        self.assertEqual(codec.encode(decoded), data)

    @given(st.binary(max_size=150))
    def test_arbitrary_input_decodes_or_raises_protocol_error(
        self, data: bytes
    ) -> None:
        self.assert_decode_contract(COMMANDS, data)
        self.assert_decode_contract(SERVER_MESSAGES, data)

    @given(
        st.sampled_from(COMMAND_FIXTURES + SERVER_FIXTURES),
        st.integers(0, 1000),
        st.integers(0, 255),
    )
    def test_mutated_valid_bodies_have_only_documented_decode_failures(
        self, fixture: tuple[object, bytes], offset: int, byte: int
    ) -> None:
        _, body = fixture
        pos = offset % len(body)
        mutated = body[:pos] + bytes([byte]) + body[pos + 1 :]
        self.assert_decode_contract(COMMANDS, mutated)
        self.assert_decode_contract(SERVER_MESSAGES, mutated)


@dataclass(frozen=True)
class Pair:
    name: str
    value: bytes


type OneMessage = Read
type NestedMessages = OneMessage | Nil
type PairMessage = Pair
type NotDataclass = str


class CodecDeclarationTests(unittest.TestCase):
    def test_nested_aliases_are_covered_and_max_body_is_exact(self) -> None:
        codec = Codec(
            "test",
            {Read: msg(b"G", key=text("key", 2)), Nil: msg(b"N")},
            covers=NestedMessages,
        )
        self.assertEqual(codec.max_body, 5 + 4 + 8)
        self.assertEqual(codec.decode(codec.encode(Read("😀😀"))), Read("😀😀"))

    def test_missing_and_unexpected_union_members_are_rejected(self) -> None:
        for table in (
            {},
            {Nil: msg(b"N")},
            {Read: msg(b"G", key=text("key", 2)), Nil: msg(b"N")},
        ):
            with (
                self.subTest(table=table),
                self.assertRaisesRegex(TypeError, "table mismatch"),
            ):
                Codec("test", table, covers=OneMessage)

    def test_non_dataclass_registration_is_rejected(self) -> None:
        with self.assertRaisesRegex(TypeError, "not a dataclass"):
            Codec("test", {str: msg(b"S")}, covers=NotDataclass)

    def test_duplicate_and_oversized_tags_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate"):
            Codec(
                "test",
                {Read: msg(b"G", key=text("key", 2)), Nil: msg(b"G")},
                covers=NestedMessages,
            )
        with self.assertRaisesRegex(ValueError, "tag longer"):
            Codec(
                "test",
                {Read: msg(b"x" * (MAX_TAG_BYTES + 1), key=text("key", 2))},
                covers=OneMessage,
            )

    def test_field_names_order_and_annotations_must_match(self) -> None:
        for entry in (
            msg(b"P", name=text("name", 3)),
            msg(b"P", value=BLOB, name=text("name", 3)),
            msg(b"P", name=text("name", 3), wrong=BLOB),
            msg(b"P", name=BLOB, value=BLOB),
        ):
            with self.subTest(entry=entry), self.assertRaises(TypeError):
                Codec("test", {Pair: entry}, covers=PairMessage)

    def test_unregistered_outgoing_type_is_a_protocol_error(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "unsupported command"):
            COMMANDS.encode(cast(Command, Ok()))
