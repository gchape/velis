import random
import sys
import unittest
import zlib

from velis.codec import CodecChain, Deflate, LengthPrefix
from velis.errors import ProtocolError


class CompressionTests(unittest.TestCase):
    def test_roundtrips_and_standard_zlib_interoperability(self) -> None:
        rng = random.Random(42)
        for level in (-1, 0, 1, 6, 9):
            codec = Deflate(level=level)
            for data in (b"", bytes(range(256)), rng.randbytes(4096), b"x" * (1 << 20)):
                with self.subTest(level=level, length=len(data)):
                    encoded = codec.encode(data)
                    self.assertEqual(codec.decode(encoded), data)
                    self.assertEqual(zlib.decompress(encoded), data)
                    self.assertEqual(codec.decode(zlib.compress(data)), data)
        self.assertLess(len(Deflate().encode(b"hello" * 1000)), 5000)

    def test_limits_are_inclusive_and_apply_in_both_directions(self) -> None:
        for limit in (0, 8, 1024):
            codec = Deflate(max_length=limit)
            data = b"x" * limit
            self.assertEqual(codec.decode(codec.encode(data)), data)
            with self.assertRaisesRegex(ProtocolError, "exceeds limit"):
                codec.encode(data + b"x")
            with self.assertRaisesRegex(ProtocolError, "exceeds limit"):
                codec.decode(zlib.compress(data + b"x"))
        with self.assertRaisesRegex(ProtocolError, "exceeds limit"):
            Deflate(max_length=8).decode(zlib.compress(b"x" * (8 << 20)))

    def test_rejects_truncation_corruption_and_trailing_streams(self) -> None:
        codec = Deflate()
        encoded = codec.encode(b"hello" * 100)
        malformed = [encoded[:end] for end in range(len(encoded))]
        malformed += [
            b"garbage",
            encoded[:-1] + bytes([encoded[-1] ^ 1]),
            encoded + b"x",
            encoded + codec.encode(b"second"),
        ]
        for data in malformed:
            with self.subTest(data=data), self.assertRaises(ProtocolError):
                codec.decode(data)
        self.assertEqual(codec.decode(encoded), b"hello" * 100)

    def test_composes_with_framing(self) -> None:
        chain = CodecChain.of(Deflate()) | LengthPrefix(4096)
        data = b"repeat" * 1000
        self.assertEqual(chain.decode(chain.encode(data)), data)

    def test_arbitrary_bytes_only_raise_protocol_errors(self) -> None:
        rng = random.Random(9)
        codec = Deflate(max_length=1024)
        for _ in range(1000):
            data = rng.randbytes(rng.randrange(128))
            try:
                decoded = codec.decode(data)
            except ProtocolError:
                continue
            self.assertLessEqual(len(decoded), codec.max_length)

    def test_invalid_configuration(self) -> None:
        for level in (-2, 10):
            with self.assertRaises(ValueError):
                Deflate(level=level)
        for limit in (-1, sys.maxsize):
            with self.assertRaises(ValueError):
                Deflate(max_length=limit)
