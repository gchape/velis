import unittest
from dataclasses import dataclass

from tests.codec.helpers import Reverse
from velis.codec import (
    U32,
    Choice,
    CompositeCodec,
    FixedLengthField,
    Identity,
    LengthPrefixedField,
    Record,
    UInt,
)
from velis.errors import ProtocolError


@dataclass(frozen=True)
class Payload:
    data: bytes


class NestedEncodingTests(unittest.TestCase):
    def test_nested_framing_preserves_the_wire_layout(self) -> None:
        data = b"\x00\xff" * 8192
        field = LengthPrefixedField(Identity(), len(data), prefix=U32)
        choice = Choice("payload", {"P": Record(Payload, field)})
        framing = LengthPrefixedField(Identity(), len(data) + 32, prefix=U32)
        codec = CompositeCodec.of(choice) | framing
        value = Payload(data)
        body = b"\x00\x00\x00\x01P" + len(data).to_bytes(4) + data
        expected = len(body).to_bytes(4) + body
        self.assertEqual(codec.encode(value), expected)
        self.assertEqual(codec.decode(expected), value)

    def test_nested_record_lengths_count_all_parts(self) -> None:
        @dataclass
        class Container:
            value: Payload
            count: int

        inner = Record(Payload, LengthPrefixedField(Identity(), 3, prefix=UInt(1)))
        outer = Record(
            Container,
            LengthPrefixedField(inner, 4, prefix=UInt(1)),
            FixedLengthField(UInt(2), 2),
        )
        value = Container(Payload(b"abc"), 258)
        self.assertEqual(outer.encode(value), b"\x04\x03abc\x01\x02")
        self.assertEqual(outer.decode(outer.encode(value)), value)
        with self.assertRaisesRegex(ProtocolError, "field too long: 4"):
            LengthPrefixedField(inner, 3, prefix=UInt(1)).encode(value.value)

    def test_nested_length_prefixes_count_encoded_bytes(self) -> None:
        payload = b"abc"
        field = LengthPrefixedField(Identity(), 3, prefix=UInt(1))
        middle = LengthPrefixedField(field, 4, prefix=UInt(1))
        codec = LengthPrefixedField(middle, 5, prefix=UInt(1))
        self.assertEqual(codec.encode(payload), b"\x05\x04\x03abc")
        self.assertEqual(codec.decode(codec.encode(payload)), payload)

    def test_custom_transforms_receive_complete_bytes(self) -> None:
        inner = LengthPrefixedField(Identity(), 3, prefix=UInt(1))
        value_codec = CompositeCodec.of(inner) | Reverse()
        outer = LengthPrefixedField(Reverse(), 10, prefix=UInt(1))
        codec = CompositeCodec.of(value_codec) | outer
        self.assertEqual(codec.encode(b"abc"), b"\x04\x03abc")
        self.assertEqual(codec.decode(codec.encode(b"abc")), b"abc")

    def test_custom_encoder_overrides_are_preserved(self) -> None:
        class ReversedRecord(Record[Payload]):
            def encode(self, value: Payload) -> bytes:
                return super().encode(value)[::-1]

            def decode(self, data: bytes) -> Payload:
                return super().decode(data[::-1])

        class DerivedRecord(ReversedRecord):
            pass

        class ReversedIdentity(Identity):
            def encode(self, value: bytes) -> bytes:
                return value[::-1]

            def decode(self, data: bytes) -> bytes:
                return data[::-1]

        for cls in (ReversedRecord, DerivedRecord):
            with self.subTest(cls=cls):
                record = cls(
                    Payload, LengthPrefixedField(Identity(), 3, prefix=UInt(1))
                )
                codec = LengthPrefixedField(record, 4, prefix=UInt(1))
                self.assertEqual(codec.encode(Payload(b"abc")), b"\x04cba\x03")
                self.assertEqual(
                    codec.decode(codec.encode(Payload(b"abc"))), Payload(b"abc")
                )

        field = LengthPrefixedField(ReversedIdentity(), 3, prefix=UInt(1))
        self.assertEqual(field.encode(b"abc"), b"\x03cba")
        self.assertEqual(field.decode(field.encode(b"abc")), b"abc")

    def test_composite_subclass_overrides_remain_composition_steps(self) -> None:
        class ReversedComposite(CompositeCodec[bytes, bytes]):
            def encode(self, value: bytes) -> bytes:
                return super().encode(value)[::-1]

            def decode(self, data: bytes) -> bytes:
                return super().decode(data[::-1])

        custom = ReversedComposite((Identity(),))
        framing = LengthPrefixedField(Identity(), 3, prefix=UInt(1))
        for codec in (
            CompositeCodec.of(custom) | framing,
            CompositeCodec((custom, framing)),
            CompositeCodec.of(Identity()) | custom | framing,
        ):
            with self.subTest(codec=codec):
                self.assertEqual(codec.encode(b"abc"), b"\x03cba")
                self.assertEqual(codec.decode(codec.encode(b"abc")), b"abc")
