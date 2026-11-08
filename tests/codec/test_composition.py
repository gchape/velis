import unittest
from unittest.mock import patch

from tests.codec.helpers import Prefix, Reverse
from velis.codec import CompositeCodec, Identity


class CompositionTests(unittest.TestCase):
    def test_identity_preserves_bytes(self) -> None:
        codec = Identity()
        for data in (b"", bytes(range(256))):
            with self.subTest(data=data):
                self.assertIs(codec.encode(data), data)
                self.assertIs(codec.decode(data), data)

    def test_encode_order_and_reverse_decode_order(self) -> None:
        chain = CompositeCodec.of(Prefix()) | Reverse()
        self.assertEqual(chain.encode(b"ab"), b"bax")
        self.assertEqual(chain.decode(b"bax"), b"ab")

    def test_roundtrip_binary_empty_and_large_payloads(self) -> None:
        chain = CompositeCodec.of(Prefix()) | Reverse()
        for data in (b"", bytes(range(256)), b"abc\x00" * 100_000):
            with self.subTest(length=len(data)):
                self.assertEqual(chain.decode(chain.encode(data)), data)

    def test_appending_codec_preserves_previous_steps(self) -> None:
        original = CompositeCodec.of(Prefix())
        extended = original | Reverse()
        self.assertEqual(extended.encode(b"ab"), b"bax")
        self.assertEqual(extended.decode(b"bax"), b"ab")
        self.assertEqual(original.encode(b"ab"), b"xab")

    def test_chain_composition_flattens_in_written_order(self) -> None:
        prefix, reverse = Prefix(), Reverse()
        combined = CompositeCodec.of(prefix) | CompositeCodec.of(reverse)
        self.assertEqual(combined.codecs, (prefix, reverse))
        self.assertEqual(combined.encode(b"ab"), b"bax")
        self.assertEqual(combined.decode(b"bax"), b"ab")

    def test_build_chain_from_identity(self) -> None:
        chain = CompositeCodec.of(Identity()) | Prefix() | Reverse()
        self.assertEqual(chain.encode(b"ab"), b"bax")
        self.assertEqual(chain.decode(b"bax"), b"ab")

    def test_composition_with_identity(self) -> None:
        chain = CompositeCodec.of(Prefix())
        for combined in (
            chain | Identity(),
            CompositeCodec.of(Identity()) | chain,
        ):
            self.assertEqual(combined.encode(b"ab"), b"xab")
            self.assertEqual(combined.decode(b"xab"), b"ab")

    def test_errors_propagate_and_stop_processing(self) -> None:
        prefix, reverse = Prefix(), Reverse()
        chain = CompositeCodec.of(prefix) | reverse
        for operation, first, second in (
            ("encode", prefix, reverse),
            ("decode", reverse, prefix),
        ):
            failure = ValueError("codec failed")
            with (
                self.subTest(operation=operation),
                patch.object(first, operation, side_effect=failure),
                patch.object(second, operation) as remaining,
                self.assertRaises(ValueError) as raised,
            ):
                getattr(chain, operation)(b"ab")
            self.assertIs(raised.exception, failure)
            remaining.assert_not_called()
