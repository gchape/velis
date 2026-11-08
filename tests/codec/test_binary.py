import unittest
from typing import assert_type

from tests.codec.helpers import Reverse
from velis.codec import (
    CompositeCodec,
    Identity,
    LengthPrefixedField,
    UInt,
    Utf8,
    Validate,
)
from velis.errors import ProtocolError


class BinaryCodecTests(unittest.TestCase):
    def test_typed_chain_composes_different_representations(self) -> None:
        chain = (
            CompositeCodec.of(Utf8())
            | Reverse()
            | LengthPrefixedField(Identity(), 64, prefix=UInt(4))
        )
        assert_type(chain, CompositeCodec[str, bytes])
        self.assertEqual(chain.encode("é"), b"\x00\x00\x00\x02\xa9\xc3")
        self.assertEqual(chain.decode(chain.encode("ქართული")), "ქართული")
        assert_type(chain.decode(b"\x00\x00\x00\x00"), str)

    def test_typed_chain_composition_and_identity(self) -> None:
        chain = CompositeCodec.of(Utf8()) | CompositeCodec.of(Reverse())
        assert_type(chain, CompositeCodec[str, bytes])
        wrapped = chain | Identity()
        assert_type(wrapped, CompositeCodec[str, bytes])
        self.assertEqual(wrapped.decode(wrapped.encode("hello")), "hello")
        self.assertEqual(CompositeCodec.of(wrapped).codecs, wrapped.codecs)

    def test_rules_validate_both_directions_at_their_position(self) -> None:
        def nonempty(value: str) -> None:
            if not value:
                raise ValueError("empty text")

        chain = CompositeCodec.of(Validate(nonempty)) | Utf8() | Reverse()
        assert_type(chain, CompositeCodec[str, bytes])
        self.assertEqual(chain.decode(chain.encode("hello")), "hello")
        with self.assertRaisesRegex(ValueError, "empty text"):
            chain.encode("")
        with self.assertRaisesRegex(ValueError, "empty text"):
            chain.decode(b"")

    def test_unsigned_integer_boundaries_and_exact_width(self) -> None:
        for length in (1, 3, 4, 8):
            codec = UInt(length=length)
            bits = length * 8
            for value in (0, (1 << bits) - 1):
                encoded = codec.encode(value)
                self.assertEqual(len(encoded), length)
                self.assertEqual(codec.decode(encoded), value)
            self.assertEqual(codec.encode(1), b"\x00" * (length - 1) + b"\x01")
            for value in (-1, 1 << bits):
                with self.assertRaises(ProtocolError):
                    codec.encode(value)
            for data in (b"", b"\x00" * (length - 1), b"\x00" * (length + 1)):
                with self.assertRaises(ProtocolError):
                    codec.decode(data)
        for length in (0, -1):
            with self.assertRaises(ValueError):
                UInt(length)

    def test_length_prefix_limits_and_malformed_bodies(self) -> None:
        codec = LengthPrefixedField(Identity(), 3, prefix=UInt(4))
        for data in (b"", b"abc"):
            self.assertEqual(codec.decode(codec.encode(data)), data)
        with self.assertRaisesRegex(ProtocolError, "field too long"):
            codec.encode(b"abcd")
        for data in (
            b"",
            b"\x00\x00",
            b"\x00\x00\x00\x04",
            b"\x00\x00\x00\x03ab",
            b"\x00\x00\x00\x01ab",
        ):
            with self.subTest(data=data), self.assertRaises(ProtocolError):
                codec.decode(data)
        for limit in (-1, 1 << 32):
            with self.assertRaises(ValueError):
                LengthPrefixedField(Identity(), limit, prefix=UInt(4))

    def test_utf8_rejects_invalid_data_in_both_directions(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "invalid utf-8"):
            Utf8().encode("\ud800")
        with self.assertRaisesRegex(ProtocolError, "invalid utf-8"):
            Utf8().decode(b"\xff")
