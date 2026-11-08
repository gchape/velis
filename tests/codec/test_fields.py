import unittest
from unittest.mock import Mock

from velis.codec import Identity, LengthPrefixedField, UInt, Utf8
from velis.codec.cursor import Cursor
from velis.errors import ProtocolError


class LengthPrefixedFieldTests(unittest.TestCase):
    def test_prefix_lengths_preserve_values_and_record_boundaries(self) -> None:
        for prefix_length in (1, 2, 3, 4, 8):
            field = LengthPrefixedField(Identity(), 255, prefix=UInt(prefix_length))
            for data in (b"", bytes(range(128)), b"x" * 255):
                with self.subTest(prefix_length=prefix_length, length=len(data)):
                    encoded = len(data).to_bytes(prefix_length) + data
                    self.assertEqual(field.encode(data), encoded)
                    self.assertEqual(field.decode(encoded), data)
                    cursor = Cursor(encoded + field.encode(b"next"))
                    self.assertEqual(field.read(cursor), data)
                    self.assertEqual(field.read(cursor), b"next")
                    cursor.finish()

    def test_limits_fit_the_prefix_and_count_encoded_bytes(self) -> None:
        field = LengthPrefixedField(Utf8(), 2, prefix=UInt(1))
        self.assertEqual(field.encode("é"), b"\x02\xc3\xa9")
        self.assertEqual(field.decode(b"\x02\xc3\xa9"), "é")
        with self.assertRaisesRegex(ProtocolError, "too long"):
            field.encode("éx")
        for prefix_length, limit in ((1, -1), (1, 256), (2, 1 << 16), (4, 1 << 32)):
            with (
                self.subTest(prefix_length=prefix_length, limit=limit),
                self.assertRaises(ValueError),
            ):
                LengthPrefixedField(Identity(), limit, prefix=UInt(prefix_length))

    def test_malformed_lengths_and_trailing_data_are_rejected(self) -> None:
        inner = Mock(wraps=Identity())
        field = LengthPrefixedField(inner, 10, prefix=UInt(1))
        for data in (b"", b"\x0b", b"\x03ab"):
            with self.subTest(data=data), self.assertRaises(ProtocolError):
                field.decode(data)
        for data in (b"", b"\x0b", b"\x03ab"):
            with self.subTest(data=data), self.assertRaises(ProtocolError):
                field.read(Cursor(data))
        inner.decode.assert_not_called()
        with self.assertRaisesRegex(ProtocolError, "trailing bytes"):
            field.decode(b"\x01ab")
