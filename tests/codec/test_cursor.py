import unittest

from velis.codec import Fixed, Identity, Prefixed, UInt, Utf8
from velis.codec.binary import U32
from velis.codec.cursor import Cursor
from velis.errors import ProtocolError


class CursorTests(unittest.TestCase):
    def test_mixed_fields_and_end_of_record(self) -> None:
        cursor = Cursor(b"\x01\x00\x00\x00\x02hi\x00\x00\x00\x00")
        with self.assertRaisesRegex(ProtocolError, "trailing bytes"):
            cursor.finish()
        self.assertEqual(Fixed(UInt(1), 1).read(cursor), 1)
        self.assertEqual(Prefixed(Utf8(), 64).read(cursor), "hi")
        self.assertEqual(Prefixed(Identity(), 64).read(cursor), b"")
        cursor.finish()
        with self.assertRaisesRegex(ProtocolError, "truncated record"):
            Fixed(UInt(1), 1).read(cursor)

    def test_prefixed_field_rejects_truncated_header_and_body(self) -> None:
        field = b"\x00\x00\x00\x03abc"
        for length in range(len(field)):
            with (
                self.subTest(length=length),
                self.assertRaisesRegex(ProtocolError, "truncated record"),
            ):
                Prefixed(Identity(), 64).read(Cursor(field[:length]))

    def test_truncated_integer_read_preserves_position(self) -> None:
        cursor = Cursor(b"\x01\x02\x03")
        self.assertEqual(Fixed(UInt(1), 1).read(cursor), 1)
        with self.assertRaises(ProtocolError):
            Fixed(U32, 4).read(cursor)
        self.assertEqual(Fixed(UInt(2), 2).read(cursor), 0x0203)
        cursor.finish()
