import dataclasses
import unittest

from velis.codec import CodecChain


def identity(data):
    return data


def reverse_bytes(data):
    return data[::-1]


def add_prefix(data):
    return b"x" + data


def strip_prefix(data):
    return data[1:]


def boom(data):
    raise ValueError("boom")


class Fn:
    """Minimal codec built from two functions; optionally records calls."""

    def __init__(self, name, encode=identity, decode=identity, log=None):
        self.name = name
        self._encode = encode
        self._decode = decode
        self._log = log

    def encode(self, data):
        if self._log is not None:
            self._log.append(f"encode:{self.name}")
        return self._encode(data)

    def decode(self, data):
        if self._log is not None:
            self._log.append(f"decode:{self.name}")
        return self._decode(data)


prefix = Fn("p", add_prefix, strip_prefix)
reverse = Fn("r", reverse_bytes, reverse_bytes)


class NameTests(unittest.TestCase):
    def test_empty_chain_has_empty_name(self):
        self.assertEqual(CodecChain().name, "")

    def test_single_codec(self):
        self.assertEqual(CodecChain((prefix,)).name, "p")

    def test_names_joined_with_underscore_in_order(self):
        chain = CodecChain((Fn("a"), Fn("b"), Fn("c")))
        self.assertEqual(chain.name, "a_b_c")

    def test_order_changes_name(self):
        self.assertEqual(CodecChain((prefix, reverse)).name, "p_r")
        self.assertEqual(CodecChain((reverse, prefix)).name, "r_p")


class EncodeDecodeTests(unittest.TestCase):
    def test_empty_chain_is_identity(self):
        chain = CodecChain()
        self.assertEqual(chain.encode(b"ab"), b"ab")
        self.assertEqual(chain.decode(b"ab"), b"ab")

    def test_empty_chain_handles_empty_data(self):
        self.assertEqual(CodecChain().encode(b""), b"")
        self.assertEqual(CodecChain().decode(b""), b"")

    def test_single_codec(self):
        chain = CodecChain((prefix,))
        self.assertEqual(chain.encode(b"ab"), b"xab")
        self.assertEqual(chain.decode(b"xab"), b"ab")

    def test_encode_runs_codecs_in_order(self):
        log = []
        chain = CodecChain(
            (Fn("p", add_prefix, log=log), Fn("r", reverse_bytes, log=log))
        )
        # prefix -> b"xab", then reverse -> b"bax"
        self.assertEqual(chain.encode(b"ab"), b"bax")
        self.assertEqual(log, ["encode:p", "encode:r"])

    def test_decode_runs_codecs_in_reverse_order(self):
        log = []
        chain = CodecChain(
            (
                Fn("p", decode=strip_prefix, log=log),
                Fn("r", decode=reverse_bytes, log=log),
            )
        )
        # reverse -> b"xab", then strip prefix -> b"ab"
        self.assertEqual(chain.decode(b"bax"), b"ab")
        self.assertEqual(log, ["decode:r", "decode:p"])

    def test_order_matters(self):
        self.assertEqual(CodecChain((prefix, reverse)).encode(b"ab"), b"bax")
        self.assertEqual(CodecChain((reverse, prefix)).encode(b"ab"), b"xba")

    def test_each_codec_receives_previous_output(self):
        seen = []

        def spy(data):
            seen.append(data)
            return data + b"!"

        chain = CodecChain((Fn("a", spy), Fn("b", spy)))
        self.assertEqual(chain.encode(b"x"), b"x!!")
        self.assertEqual(seen, [b"x", b"x!"])

    def test_same_codec_can_appear_twice(self):
        chain = CodecChain((reverse, reverse))
        self.assertEqual(chain.encode(b"abc"), b"abc")
        self.assertEqual(chain.name, "r_r")

    def test_roundtrip(self):
        chain = CodecChain((prefix, reverse, Fn("i")))
        samples = [b"", b"a", b"hello world", bytes(range(256)), b"abc" * 100_000]
        for data in samples:
            with self.subTest(length=len(data)):
                self.assertEqual(chain.decode(chain.encode(data)), data)

    def test_chain_is_stateless_between_calls(self):
        chain = CodecChain((prefix, reverse))
        first = chain.encode(b"ab")
        self.assertEqual(chain.encode(b"ab"), first)
        self.assertEqual(chain.decode(first), b"ab")
        self.assertEqual(chain.decode(first), b"ab")


class ErrorTests(unittest.TestCase):
    def test_encode_error_gets_note(self):
        chain = CodecChain((prefix, Fn("bad", encode=boom)))
        with self.assertRaises(ValueError) as cm:
            chain.encode(b"x")
        self.assertEqual(
            cm.exception.__notes__,
            ["encode failed in codec 'bad' of chain 'p_bad'"],
        )

    def test_decode_error_gets_note(self):
        chain = CodecChain((prefix, Fn("bad", decode=boom)))
        with self.assertRaises(ValueError) as cm:
            chain.decode(b"x")
        self.assertEqual(
            cm.exception.__notes__,
            ["decode failed in codec 'bad' of chain 'p_bad'"],
        )

    def test_original_exception_object_is_reraised(self):
        original = ValueError("boom")

        def raiser(_):
            raise original

        chain = CodecChain((Fn("bad", encode=raiser, decode=raiser),))
        for method in (chain.encode, chain.decode):
            with self.subTest(method=method.__name__):
                with self.assertRaises(ValueError) as cm:
                    method(b"x")
                self.assertIs(cm.exception, original)

    def test_exception_types_are_preserved(self):
        for exc_type in (ValueError, KeyError, TypeError, OSError, RuntimeError):
            with self.subTest(exc_type=exc_type.__name__):

                def raiser(_, exc_type=exc_type):
                    raise exc_type("x")

                chain = CodecChain((Fn("bad", encode=raiser),))
                with self.assertRaises(exc_type) as cm:
                    chain.encode(b"x")
                self.assertEqual(len(cm.exception.__notes__), 1)

    def test_encode_stops_at_first_failure(self):
        log = []
        chain = CodecChain(
            (Fn("a", log=log), Fn("bad", encode=boom, log=log), Fn("c", log=log))
        )
        with self.assertRaises(ValueError):
            chain.encode(b"x")
        self.assertEqual(log, ["encode:a", "encode:bad"])

    def test_decode_stops_at_first_failure(self):
        log = []
        chain = CodecChain(
            (Fn("a", log=log), Fn("bad", decode=boom, log=log), Fn("c", log=log))
        )
        with self.assertRaises(ValueError):
            chain.decode(b"x")
        # reversed order: c runs, bad fails, a never runs
        self.assertEqual(log, ["decode:c", "decode:bad"])

    def test_note_names_the_failing_codec_wherever_it_sits(self):
        for position in range(3):
            with self.subTest(position=position):
                codecs_ = [Fn("a"), Fn("b"), Fn("c")]
                codecs_[position] = Fn("bad", encode=boom)
                chain = CodecChain(tuple(codecs_))
                with self.assertRaises(ValueError) as cm:
                    chain.encode(b"x")
                self.assertEqual(
                    cm.exception.__notes__,
                    [f"encode failed in codec 'bad' of chain {chain.name!r}"],
                )

    def test_nested_chain_failure_gets_inner_then_outer_note(self):
        inner = CodecChain((Fn("a"), Fn("bad", encode=boom)))
        outer = CodecChain((prefix, inner))
        with self.assertRaises(ValueError) as cm:
            outer.encode(b"x")
        self.assertEqual(
            cm.exception.__notes__,
            [
                "encode failed in codec 'bad' of chain 'a_bad'",
                "encode failed in codec 'a_bad' of chain 'p_a_bad'",
            ],
        )

    def test_error_does_not_break_later_calls(self):
        flaky = {"fail": True}

        def sometimes(data):
            if flaky["fail"]:
                raise ValueError("boom")
            return data

        chain = CodecChain((Fn("f", encode=sometimes),))
        with self.assertRaises(ValueError):
            chain.encode(b"x")
        flaky["fail"] = False
        self.assertEqual(chain.encode(b"x"), b"x")


class CompositionTests(unittest.TestCase):
    def test_chain_or_codec_appends(self):
        self.assertEqual((CodecChain((prefix,)) | reverse).codecs, (prefix, reverse))

    def test_codec_or_chain_prepends(self):
        self.assertEqual((prefix | CodecChain((reverse,))).codecs, (prefix, reverse))

    def test_building_from_empty_chain(self):
        a, b, c = Fn("a"), Fn("b"), Fn("c")
        chain = CodecChain() | a | b | c
        self.assertEqual(chain.codecs, (a, b, c))
        self.assertEqual(chain.name, "a_b_c")

    def test_prepending_and_appending_together(self):
        a, b, c = Fn("a"), Fn("b"), Fn("c")
        chain = a | CodecChain((b,)) | c
        self.assertEqual(chain.codecs, (a, b, c))

    def test_chain_or_chain_nests_rather_than_flattens(self):
        inner = CodecChain((reverse,))
        outer = CodecChain((prefix,)) | inner
        self.assertEqual(outer.codecs, (prefix, inner))
        self.assertEqual(outer.encode(b"ab"), b"bax")
        self.assertEqual(outer.decode(b"bax"), b"ab")

    def test_or_returns_new_chain_and_does_not_mutate(self):
        original = CodecChain((prefix,))
        extended = original | reverse
        self.assertIsInstance(extended, CodecChain)
        self.assertIsNot(extended, original)
        self.assertEqual(original.codecs, (prefix,))

    def test_ror_returns_new_chain_and_does_not_mutate(self):
        original = CodecChain((reverse,))
        extended = prefix | original
        self.assertIsInstance(extended, CodecChain)
        self.assertEqual(original.codecs, (reverse,))

    def test_composed_chain_encodes_in_written_order(self):
        chain = CodecChain() | prefix | reverse
        self.assertEqual(chain.encode(b"ab"), b"bax")


class ValueSemanticsTests(unittest.TestCase):
    def test_default_is_empty_tuple(self):
        self.assertEqual(CodecChain().codecs, ())

    def test_frozen(self):
        chain = CodecChain((prefix,))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            chain.codecs = ()  # ty: ignore[invalid-assignment]

    def test_uses_slots(self):
        self.assertFalse(hasattr(CodecChain(), "__dict__"))

    def test_equality_and_hash_by_codecs(self):
        a = CodecChain((prefix, reverse))
        b = CodecChain((prefix, reverse))
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertNotEqual(a, CodecChain((reverse, prefix)))
        self.assertNotEqual(a, CodecChain())


if __name__ == "__main__":
    unittest.main()
