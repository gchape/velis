import unittest
from typing import assert_type
from unittest.mock import patch

from tests.codec.helpers import Prefix, Reverse
from velis.codec import CompositeCodec, Identity, LengthPrefixedField, UInt, Utf8


class CompositeCodecTests(unittest.TestCase):
    def test_nested_groups_preserve_structure_and_execution_order(self) -> None:
        group: CompositeCodec[bytes, bytes] = CompositeCodec((Prefix(), Reverse()))
        codec: CompositeCodec[bytes, bytes] = CompositeCodec(
            (group, LengthPrefixedField(Identity(), 64, prefix=UInt(4)))
        )
        self.assertIs(codec.codecs[0], group)
        self.assertEqual(codec.encode(b"ab"), b"\x00\x00\x00\x03bax")
        self.assertEqual(codec.decode(codec.encode(b"ab")), b"ab")

    def test_typed_composition_accepts_individual_codecs(self) -> None:
        codec = CompositeCodec.of(Utf8()) | LengthPrefixedField(
            Identity(), 64, prefix=UInt(4)
        )
        assert_type(codec, CompositeCodec[str, bytes])
        self.assertEqual(codec.decode(codec.encode("hello")), "hello")

    def test_empty_composite_is_identity(self) -> None:
        codec: CompositeCodec[bytes, bytes] = CompositeCodec()
        data = b"abc"
        self.assertIs(codec.encode(data), data)
        self.assertIs(codec.decode(data), data)

    def test_nested_errors_propagate_without_running_remaining_children(self) -> None:
        first, second, third = Prefix(), Reverse(), Prefix()
        codec: CompositeCodec[bytes, bytes] = CompositeCodec(
            (CompositeCodec((first, second)), third)
        )
        for operation, preceding, remaining in (
            ("encode", first, third),
            ("decode", third, first),
        ):
            failure = ValueError("failed inside composite")
            with (
                self.subTest(operation=operation),
                patch.object(preceding, operation, side_effect=failure),
                patch.object(remaining, operation) as skipped,
                self.assertRaises(ValueError) as raised,
            ):
                getattr(codec, operation)(b"abc")
            self.assertIs(raised.exception, failure)
            skipped.assert_not_called()
