import unittest

from velis.codec import CodecChain, LengthPrefix, UInt, Utf8
from velis.codec.binary import U32
from velis.codec.cursor import Cursor
from velis.errors import ProtocolError


class CursorTests(unittest.TestCase):
    def test_mixed_fields_and_end_of_record(self) -> None:
        cursor = Cursor(b"\x01\x00\x00\x00\x02hi\x00\x00\x00\x00")
        with self.assertRaisesRegex(ProtocolError, "trailing bytes"):
            cursor.finish()
        self.assertEqual(cursor.read_uint(UInt(1)), 1)
        self.assertEqual(cursor.read(CodecChain.of(Utf8()) | LengthPrefix(64)), "hi")
        self.assertEqual(cursor.read(LengthPrefix(64)), b"")
        cursor.finish()
        with self.assertRaisesRegex(ProtocolError, "truncated record"):
            cursor.read_uint(UInt(1))

    def test_prefixed_field_rejects_truncated_header_and_body(self) -> None:
        field = b"\x00\x00\x00\x03abc"
        for length in range(len(field)):
            with (
                self.subTest(length=length),
                self.assertRaisesRegex(ProtocolError, "truncated record"),
            ):
                Cursor(field[:length]).read(LengthPrefix(64))

    def test_truncated_integer_read_preserves_position(self) -> None:
        cursor = Cursor(b"\x01\x02\x03")
        self.assertEqual(cursor.read_uint(UInt(1)), 1)
        with self.assertRaises(ProtocolError):
            cursor.read_uint(U32)
        self.assertEqual(cursor.read_uint(UInt(2)), 0x0203)
        cursor.finish()
