import unittest
from dataclasses import dataclass, field

from velis.codec import Choice, FixedLengthField, Record, UInt


class RecordTests(unittest.TestCase):
    def test_dataclass_without_match_metadata_roundtrips(self) -> None:
        @dataclass(match_args=False)
        class Value:
            count: int

        codec = Record(Value, FixedLengthField(UInt(1), 1))
        self.assertEqual(codec.encode(Value(7)), b"\x07")
        self.assertEqual(codec.decode(b"\x07"), Value(7))

    def test_pattern_matching_order_does_not_change_field_order(self) -> None:
        @dataclass
        class Pair:
            first: int
            second: int

            __match_args__ = ("second", "first")

        codec = Record(Pair, FixedLengthField(UInt(1), 1), FixedLengthField(UInt(1), 1))
        self.assertEqual(codec.encode(Pair(1, 2)), b"\x01\x02")
        self.assertEqual(codec.decode(b"\x01\x02"), Pair(1, 2))

    def test_keyword_only_fields_are_rejected_before_data_can_be_lost(self) -> None:
        @dataclass
        class Required:
            count: int
            label: str = field(kw_only=True)

        @dataclass
        class Defaulted:
            count: int
            label: str = field(kw_only=True, default="default")

        for cls in (Required, Defaulted):
            with (
                self.subTest(cls=cls),
                self.assertRaisesRegex(ValueError, "positional init fields"),
            ):
                Record(cls, FixedLengthField(UInt(1), 1))

    def test_constructor_parameters_define_the_record_layout(self) -> None:
        @dataclass(init=False)
        class DifferentConstructor:
            count: int

            def __init__(self) -> None:
                self.count = 0

        codec = Record(DifferentConstructor)
        value = DifferentConstructor()
        self.assertEqual(codec.encode(value), b"")
        self.assertEqual(codec.decode(b""), value)
        with self.assertRaisesRegex(ValueError, "0 fields but 1 codecs"):
            Record(DifferentConstructor, FixedLengthField(UInt(1), 1))

    def test_derived_fields_are_reconstructed_without_being_encoded(self) -> None:
        @dataclass
        class Value:
            count: int
            doubled: int = field(init=False)

            def __post_init__(self) -> None:
                self.doubled = self.count * 2

        codec = Record(Value, FixedLengthField(UInt(1), 1))
        value = Value(7)
        self.assertEqual(codec.encode(value), b"\x07")
        self.assertEqual(codec.decode(b"\x07"), value)
        self.assertEqual(codec.decode(b"\x07").doubled, 14)

    def test_plain_classes_use_positional_constructor_attributes(self) -> None:
        class Value:
            def __init__(self, count: int, /) -> None:
                self.count = count

        codec = Record(Value, FixedLengthField(UInt(1), 1))
        self.assertEqual(codec.encode(Value(7)), b"\x07")
        self.assertEqual(codec.decode(b"\x07").count, 7)

    def test_records_and_choices_compose_inside_another_record(self) -> None:
        @dataclass
        class Value:
            count: int

        @dataclass
        class Container:
            value: Value
            tail: int

        inner = Record(Value, FixedLengthField(UInt(1), 1))
        choice: Choice[Value] = Choice("value", {"V": inner})
        for field_codec in (inner, choice):
            with self.subTest(field_codec=field_codec):
                codec = Record(Container, field_codec, FixedLengthField(UInt(1), 1))
                value = Container(Value(7), 8)
                self.assertEqual(codec.decode(codec.encode(value)), value)
