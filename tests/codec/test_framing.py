import unittest

from velis.codec import FrameDecoder, LengthPrefix
from velis.errors import ProtocolError


class FrameDecoderTests(unittest.TestCase):
    def test_fragmented_and_pipelined_frames(self) -> None:
        framing = LengthPrefix(100)
        bodies = [b"one", b"", b"three"]
        encoded = b"".join(framing.encode(body) for body in bodies)
        for chunk_size in range(1, len(encoded) + 1):
            with self.subTest(chunk_size=chunk_size):
                decoder = FrameDecoder(framing)
                decoded = []
                for offset in range(0, len(encoded), chunk_size):
                    decoder.feed(encoded[offset : offset + chunk_size])
                    decoded.extend(decoder.bodies())
                self.assertEqual(decoded, bodies)
                self.assertEqual(decoder.pending, b"")
                decoder.feed(encoded)
                self.assertEqual(list(decoder.bodies()), bodies)

    def test_oversized_header_fails_before_body_arrives(self) -> None:
        decoder = FrameDecoder(LengthPrefix(10))
        decoder.feed((11).to_bytes(4))
        with self.assertRaisesRegex(ProtocolError, "too long"):
            list(decoder.bodies())

    def test_pending_bytes_exclude_completed_frames(self) -> None:
        framing = LengthPrefix(100)
        decoder = FrameDecoder(framing)
        incomplete = framing.encode(b"abcdef")
        decoder.feed(framing.encode(b"first") + incomplete[:6])
        self.assertEqual(list(decoder.bodies()), [b"first"])
        self.assertEqual(decoder.pending, incomplete[:6])
        decoder.feed(incomplete[6:])
        self.assertEqual(list(decoder.bodies()), [b"abcdef"])
        self.assertEqual(decoder.pending, b"")
