import unittest
from dataclasses import dataclass

from velis.codec import CodecChain, LengthPrefix, Utf8
from velis.codec.records import Field, Record, TaggedCodec
from velis.errors import ProtocolError

TEXT = CodecChain.of(Utf8()) | LengthPrefix(64)


@dataclass(frozen=True)
class Pair:
    first: str
    second: str


@dataclass(frozen=True, kw_only=True)
class Named:
    value: str


class RecordCodecTests(unittest.TestCase):
    def test_wire_order_is_independent_of_constructor_order(self) -> None:
        codec = TaggedCodec(
            "pair",
            TEXT,
            (Record(Pair, "PAIR", (Field("second", TEXT), Field("first", TEXT))),),
        )
        value = Pair("first", "second")
        self.assertEqual(
            codec.encode(value),
            TEXT.encode("PAIR") + TEXT.encode("second") + TEXT.encode("first"),
        )
        self.assertEqual(codec.decode(codec.encode(value)), value)

    def test_keyword_only_record_roundtrip(self) -> None:
        codec = TaggedCodec(
            "named",
            TEXT,
            (Record(Named, "NAMED", (Field("value", TEXT),)),),
        )
        value = Named(value="hello")
        self.assertEqual(codec.decode(codec.encode(value)), value)

    def test_duplicate_field_names_rejected_at_schema_definition(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate field"):
            Record(Pair, "PAIR", (Field("first", TEXT), Field("first", TEXT)))

    def test_duplicate_tags_and_types_rejected_at_schema_definition(self) -> None:
        for records in (
            (Record[Pair | Named](Pair, "SAME"), Record[Pair | Named](Named, "SAME")),
            (Record[Pair | Named](Pair, "FIRST"), Record[Pair | Named](Pair, "SECOND")),
        ):
            with self.subTest(records=records), self.assertRaises(ValueError):
                TaggedCodec("duplicate", TEXT, records)

    def test_record_rejects_trailing_bytes_before_construction(self) -> None:
        codec = TaggedCodec(
            "named", TEXT, (Record(Named, "NAMED", (Field("value", TEXT),)),)
        )
        data = codec.encode(Named(value="hello"))
        with self.assertRaisesRegex(ProtocolError, "trailing bytes"):
            codec.decode(data + b"extra")
