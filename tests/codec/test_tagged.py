import unittest
from dataclasses import dataclass
from typing import TypeAliasType, cast, get_args

from velis.codec import (
    CodecChain,
    Fixed,
    Identity,
    Prefixed,
    TaggedUnion,
    UInt,
    Utf8,
    Validate,
)
from velis.codec.cursor import Cursor
from velis.core.commands import Command, Message, Reply
from velis.errors import ProtocolError
from velis.net.protocol import COMMANDS, FRAMES


def members(alias: TypeAliasType) -> set[type]:
    result: set[type] = set()
    for variant in get_args(alias.__value__):
        result |= members(variant) if isinstance(variant, TypeAliasType) else {variant}
    return result


@dataclass(frozen=True)
class Record:
    name: str
    count: int


class TaggedUnionTests(unittest.TestCase):
    def test_protocol_tables_cover_every_command_and_reply(self) -> None:
        self.assertEqual(COMMANDS.types, members(Command))
        self.assertEqual(FRAMES.types, members(Reply) | {Message})

    def test_custom_variants_mix_prefixed_fields_and_fixed_width_integers(self) -> None:
        text = Prefixed(Utf8(), 32)
        codec = TaggedUnion("record", {"R": (Record, (text, Fixed(UInt(2), 2)))})
        record = Record("é", 258)
        encoded = b"\x00\x00\x00\x01R\x00\x00\x00\x02\xc3\xa9\x01\x02"
        self.assertEqual(codec.encode(record), encoded)
        self.assertEqual(codec.decode(encoded), record)
        for malformed in (encoded[:-1], encoded + b"extra"):
            with self.subTest(malformed=malformed), self.assertRaises(ProtocolError):
                codec.decode(malformed)
        with self.assertRaisesRegex(ProtocolError, "unsupported"):
            codec.encode(cast(Record, Message("news", b"hello")))

    def test_invalid_variant_tables_fail_at_construction(self) -> None:
        with self.assertRaisesRegex(ValueError, "2 fields but 0 codecs"):
            TaggedUnion("record", {"R": (Record, ())})
        codecs = (Prefixed(Utf8(), 32), Fixed(UInt(2), 2))
        with self.assertRaisesRegex(ValueError, "listed twice"):
            TaggedUnion("record", {"R": (Record, codecs), "S": (Record, codecs)})
        with self.assertRaisesRegex(TypeError, "must be a dataclass"):
            TaggedUnion("record", {"R": (str, ())})

    def test_composed_integer_field_preserves_its_layout(self) -> None:
        def positive(value: int) -> None:
            if value <= 0:
                raise ProtocolError("must be positive")

        integer = Fixed(CodecChain.of(Validate(positive)) | UInt(2), 2)
        codec = TaggedUnion("record", {"R": (Record, (Prefixed(Utf8(), 32), integer))})
        value = Record("hello", 258)
        self.assertEqual(codec.decode(codec.encode(value)), value)
        with self.assertRaisesRegex(ProtocolError, "must be positive"):
            codec.encode(Record("hello", 0))

    def test_fields_enforce_boundaries_before_decoding(self) -> None:
        for operation in (Fixed(Identity(), 2).encode, Fixed(Identity(), 2).decode):
            with self.assertRaisesRegex(ProtocolError, "expected 2 field bytes"):
                operation(b"x")
        with self.assertRaisesRegex(ProtocolError, "too long"):
            Prefixed(Identity(), 2).read(Cursor((3).to_bytes(4)))

    def test_invalid_layouts_and_tags_fail_during_setup(self) -> None:
        with self.assertRaises(ValueError):
            Fixed(UInt(1), 0)
        with self.assertRaises(ValueError):
            Prefixed(Utf8(), -1)
        codecs = (Prefixed(Utf8(), 32), Fixed(UInt(2), 2))
        with self.assertRaisesRegex(ValueError, "invalid tag"):
            TaggedUnion("record", {"x" * 33: (Record, codecs)})

        @dataclass(kw_only=True)
        class Named:
            name: str

        with self.assertRaisesRegex(ValueError, "positional init fields"):
            TaggedUnion("record", {"N": (Named, (Prefixed(Utf8(), 32),))})
