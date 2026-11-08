import unittest

from velis.codec import U32, FixedLengthField, Identity, LengthPrefixedField, UInt, Utf8
from velis.codec.cursor import Cursor
from velis.errors import ProtocolError


class CursorTests(unittest.TestCase):
    def test_context_requires_complete_consumption_on_normal_exit(self) -> None:
        with Cursor(b"abc") as cursor:
            self.assertEqual(cursor.take(3), b"abc")
        with Cursor(b"") as cursor:
            self.assertEqual(cursor.take(0), b"")
        with (
            self.assertRaisesRegex(ProtocolError, "trailing bytes"),
            Cursor(b"abc") as cursor,
        ):
            cursor.take(1)

    def test_context_preserves_truncation_error_and_cursor_position(self) -> None:
        cursor = Cursor(b"abc")
        with (
            self.assertRaisesRegex(ProtocolError, "truncated record: need 4 bytes"),
            cursor,
        ):
            cursor.take(4)
        self.assertEqual(cursor.take(3), b"abc")

    def test_context_preserves_original_exception_with_unread_data(self) -> None:
        failure = ValueError("invalid value")
        with self.assertRaises(ValueError) as raised, Cursor(b"unread"):
            raise failure
        self.assertIs(raised.exception, failure)

    def test_mixed_fields_and_end_of_record(self) -> None:
        cursor = Cursor(b"\x01\x00\x00\x00\x02hi\x00\x00\x00\x00")
        with self.assertRaisesRegex(ProtocolError, "trailing bytes"):
            cursor.finish()
        self.assertEqual(FixedLengthField(UInt(1), length=1).read(cursor), 1)
        self.assertEqual(
            LengthPrefixedField(Utf8(), 64, prefix=UInt(4)).read(cursor), "hi"
        )
        self.assertEqual(
            LengthPrefixedField(Identity(), 64, prefix=UInt(4)).read(cursor), b""
        )
        cursor.finish()
        with self.assertRaisesRegex(ProtocolError, "truncated record"):
            FixedLengthField(UInt(1), 1).read(cursor)

    def test_prefixed_field_rejects_truncated_header_and_body(self) -> None:
        field = b"\x00\x00\x00\x03abc"
        for length in range(len(field)):
            with (
                self.subTest(length=length),
                self.assertRaisesRegex(ProtocolError, "truncated record"),
            ):
                LengthPrefixedField(Identity(), 64, prefix=UInt(4)).read(
                    Cursor(field[:length])
                )

    def test_truncated_integer_read_preserves_position(self) -> None:
        cursor = Cursor(b"\x01\x02\x03")
        self.assertEqual(FixedLengthField(UInt(1), 1).read(cursor), 1)
        with self.assertRaises(ProtocolError):
            FixedLengthField(U32, 4).read(cursor)
        self.assertEqual(FixedLengthField(UInt(2), 2).read(cursor), 0x0203)
        cursor.finish()
