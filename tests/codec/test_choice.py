import unittest
from dataclasses import dataclass
from typing import TypeAliasType, cast, get_args

from velis.codec import (
    TAG,
    UTF8,
    Choice,
    CompositeCodec,
    FixedLengthField,
    Identity,
    LengthPrefixedField,
    Record,
    UInt,
    Validate,
)
from velis.codec.cursor import Cursor
from velis.core.messages import Command, Message, Reply
from velis.errors import ProtocolError
from velis.net.protocol import COMMANDS, SERVER_MESSAGES


def members(alias: TypeAliasType) -> set[type]:
    result: set[type] = set()
    for variant in get_args(alias.__value__):
        result |= members(variant) if isinstance(variant, TypeAliasType) else {variant}
    return result


@dataclass(frozen=True)
class Row:
    name: str
    count: int


class ChoiceTests(unittest.TestCase):
    def test_bad_tag_error_is_preserved_when_body_bytes_remain(self) -> None:
        codec = Choice(
            "record",
            {
                "R": Record(
                    Row,
                    LengthPrefixedField(UTF8(), 32, prefix=UInt(4)),
                    FixedLengthField(UInt(2), 2),
                )
            },
        )
        with self.assertRaisesRegex(ProtocolError, "unknown record: 'missing'"):
            codec.decode(TAG.encode("missing") + b"unread body")

    def test_protocol_tables_cover_every_command_and_reply(self) -> None:
        self.assertEqual(COMMANDS.types, members(Command))
        self.assertEqual(SERVER_MESSAGES.types, members(Reply) | {Message})

    def test_custom_variants_mix_prefixed_fields_and_fixed_width_integers(self) -> None:
        text = LengthPrefixedField(UTF8(), 32, prefix=UInt(4))
        codec = Choice("record", {"R": Record(Row, text, FixedLengthField(UInt(2), 2))})
        record = Row("é", 258)
        encoded = b"\x00\x00\x00\x01R\x00\x00\x00\x02\xc3\xa9\x01\x02"
        self.assertEqual(codec.encode(record), encoded)
        self.assertEqual(codec.decode(encoded), record)
        for malformed in (encoded[:-1], encoded + b"extra"):
            with self.subTest(malformed=malformed), self.assertRaises(ProtocolError):
                codec.decode(malformed)
        with self.assertRaisesRegex(ProtocolError, "unsupported"):
            codec.encode(cast(Row, Message("news", b"hello")))

    def test_invalid_variant_tables_fail_at_construction(self) -> None:
        with self.assertRaisesRegex(ValueError, "2 fields but 0 codecs"):
            Record(Row)
        codecs = (
            LengthPrefixedField(UTF8(), 32, prefix=UInt(4)),
            FixedLengthField(UInt(2), 2),
        )
        with self.assertRaisesRegex(ValueError, "listed twice"):
            Choice("record", {"R": Record(Row, *codecs), "S": Record(Row, *codecs)})
        with self.assertRaises(ValueError):
            Record(str)

    def test_composed_integer_field_preserves_its_layout(self) -> None:
        def positive(value: int) -> str | None:
            if value <= 0:
                return "must be positive"
            return None

        integer = FixedLengthField(CompositeCodec.of(Validate(positive)) | UInt(2), 2)
        codec = Choice(
            "record",
            {
                "R": Record(
                    Row, LengthPrefixedField(UTF8(), 32, prefix=UInt(4)), integer
                )
            },
        )
        value = Row("hello", 258)
        self.assertEqual(codec.decode(codec.encode(value)), value)
        with self.assertRaisesRegex(ProtocolError, "must be positive"):
            codec.encode(Row("hello", 0))

    def test_fields_enforce_boundaries_before_decoding(self) -> None:
        for operation in (
            FixedLengthField(Identity(), 2).encode,
            FixedLengthField(Identity(), 2).decode,
        ):
            with self.assertRaisesRegex(ProtocolError, "expected 2 field bytes"):
                operation(b"x")
        with self.assertRaisesRegex(ProtocolError, "too long"):
            LengthPrefixedField(Identity(), 2, prefix=UInt(4)).read(
                Cursor((3).to_bytes(4))
            )

    def test_invalid_layouts_and_tags_fail_during_setup(self) -> None:
        with self.assertRaises(ValueError):
            FixedLengthField(UInt(1), 0)
        with self.assertRaises(ValueError):
            LengthPrefixedField(UTF8(), -1, prefix=UInt(4))
        codecs = (
            LengthPrefixedField(UTF8(), 32, prefix=UInt(4)),
            FixedLengthField(UInt(2), 2),
        )
        with self.assertRaisesRegex(ValueError, "invalid tag"):
            Choice("record", {"x" * 33: Record(Row, *codecs)})

        @dataclass(kw_only=True)
        class Named:
            name: str

        with self.assertRaisesRegex(ValueError, "positional init fields"):
            Record(Named, LengthPrefixedField(UTF8(), 32, prefix=UInt(4)))
