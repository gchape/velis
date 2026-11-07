import codecs
import itertools
import unittest

from velis.codec import CodecChain, register, registry
from velis.exceptions import CodecError

# codecs.lookup caches results and can't be cleared, so every test registers
# under a fresh name instead of reusing one.
_counter = itertools.count()


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


class RegistryTestCase(unittest.TestCase):
    def setUp(self):
        self.log = []
        before = set(registry._infos)
        self.addCleanup(self._forget_new_entries, before)

    @staticmethod
    def _forget_new_entries(before):
        for key in set(registry._infos) - before:
            del registry._infos[key]

    @staticmethod
    def fresh_name():
        return f"registrytest{next(_counter)}"

    def codec(self, encode=identity, decode=identity, name=None):
        return Fn(name or self.fresh_name(), encode, decode, self.log)

    def reg(self, encode=identity, decode=identity, encoding="utf-8"):
        """Register a fresh single-codec chain and return it."""
        chain = CodecChain((self.codec(encode, decode),))
        return register(chain, encoding)


class RegisterTests(RegistryTestCase):
    def test_returns_the_same_chain(self):
        chain = CodecChain((self.codec(),))
        self.assertIs(register(chain), chain)

    def test_adds_codec_info_under_chain_name(self):
        chain = self.reg()
        self.assertIn(chain.name, registry._infos)
        self.assertEqual(registry._infos[chain.name].name, chain.name)

    def test_codec_is_found_by_codecs_lookup(self):
        chain = self.reg()
        self.assertEqual(codecs.lookup(chain.name).name, chain.name)

    def test_multi_codec_chain_registers_under_joined_name(self):
        base = self.fresh_name()
        chain = register(
            CodecChain((self.codec(name=f"{base}a"), self.codec(name=f"{base}b")))
        )
        self.assertEqual(chain.name, f"{base}a_{base}b")
        self.assertEqual(codecs.lookup(chain.name).name, chain.name)

    def test_independent_registrations_do_not_interfere(self):
        one = self.reg(reverse_bytes, reverse_bytes)
        two = self.reg()
        self.assertEqual("abc".encode(one.name), b"cba")
        self.assertEqual("abc".encode(two.name), b"abc")

    def test_unregistered_name_is_not_found(self):
        with self.assertRaises(LookupError):
            codecs.lookup(self.fresh_name())

    def test_empty_chain_rejected(self):
        with self.assertRaisesRegex(ValueError, "empty chain"):
            register(CodecChain())

    def test_empty_chain_leaves_registry_unchanged(self):
        before = dict(registry._infos)
        with self.assertRaises(ValueError):
            register(CodecChain())
        self.assertEqual(registry._infos, before)

    def test_duplicate_name_rejected_with_name_in_message(self):
        chain = self.reg()
        with self.assertRaisesRegex(ValueError, "already registered") as cm:
            register(chain)
        self.assertIn(repr(chain.name), str(cm.exception))

    def test_duplicate_keeps_the_original_codec(self):
        first = self.reg(reverse_bytes, reverse_bytes)
        impostor = CodecChain((self.codec(name=first.name),))  # identity codec
        with self.assertRaises(ValueError):
            register(impostor)
        self.assertEqual("abc".encode(first.name), b"cba")

    def test_duplicate_with_different_encoding_still_rejected(self):
        chain = self.reg()
        with self.assertRaises(ValueError):
            register(chain, "latin-1")

    def test_logs_registration_with_default_encoding(self):
        chain = CodecChain((self.codec(),))
        with self.assertLogs("velis.codec.registry", level="INFO") as cm:
            register(chain)
        self.assertEqual(
            [r.getMessage() for r in cm.records],
            [f"registered codec {chain.name!r} (text encoding utf-8)"],
        )

    def test_logs_registration_with_custom_encoding(self):
        chain = CodecChain((self.codec(),))
        with self.assertLogs("velis.codec.registry", level="INFO") as cm:
            register(chain, "latin-1")
        self.assertEqual(
            [r.getMessage() for r in cm.records],
            [f"registered codec {chain.name!r} (text encoding latin-1)"],
        )

    def test_failed_registration_logs_nothing(self):
        chain = self.reg()
        with self.assertNoLogs("velis.codec.registry", level="DEBUG"):
            with self.assertRaises(ValueError):
                register(chain)
            with self.assertRaises(ValueError):
                register(CodecChain())


class RoundtripTests(RegistryTestCase):
    def test_str_and_bytes_roundtrip(self):
        chain = self.reg(reverse_bytes, reverse_bytes)
        encoded = "héllo".encode(chain.name)
        self.assertEqual(encoded, "héllo".encode()[::-1])
        self.assertEqual(encoded.decode(chain.name), "héllo")

    def test_roundtrip_of_varied_text(self):
        chain = self.reg(reverse_bytes, reverse_bytes)
        for text in ["", "ascii", "héllo", "日本語", "🙂", "a\x00b", "line\nbreak"]:
            with self.subTest(text=text):
                self.assertEqual(text.encode(chain.name).decode(chain.name), text)

    def test_identity_chain_matches_plain_encoding(self):
        chain = self.reg()
        self.assertEqual("héllo".encode(chain.name), "héllo".encode())
        self.assertEqual(b"h\xc3\xa9llo".decode(chain.name), "héllo")

    def test_multi_codec_chain_roundtrip(self):
        base = self.fresh_name()
        chain = register(
            CodecChain(
                (
                    self.codec(add_prefix, strip_prefix, name=f"{base}p"),
                    self.codec(reverse_bytes, reverse_bytes, name=f"{base}r"),
                )
            )
        )
        encoded = "ab".encode(chain.name)
        self.assertEqual(encoded, b"bax")
        self.assertEqual(encoded.decode(chain.name), "ab")

    def test_codecs_module_functions(self):
        chain = self.reg(reverse_bytes, reverse_bytes)
        self.assertEqual(codecs.encode("abc", chain.name), b"cba")
        self.assertEqual(codecs.decode(b"cba", chain.name), "abc")

    def test_repeated_calls_give_same_result(self):
        chain = self.reg(reverse_bytes, reverse_bytes)
        for _ in range(3):
            self.assertEqual("abc".encode(chain.name), b"cba")
            self.assertEqual(b"cba".decode(chain.name), "abc")


class TextEncodingTests(RegistryTestCase):
    def test_default_is_utf8(self):
        chain = self.reg()
        self.assertEqual("é".encode(chain.name), b"\xc3\xa9")

    def test_custom_encodings(self):
        cases = [
            ("latin-1", "é", b"\xe9"),
            ("utf-16-le", "aé", b"a\x00\xe9\x00"),
            ("ascii", "abc", b"abc"),
        ]
        for encoding, text, expected in cases:
            with self.subTest(encoding=encoding):
                chain = self.reg(encoding=encoding)
                self.assertEqual(text.encode(chain.name), expected)
                self.assertEqual(expected.decode(chain.name), text)

    def test_text_is_encoded_before_the_chain_runs(self):
        seen = []

        def spy(data):
            seen.append(data)
            return data

        chain = self.reg(encode=spy, encoding="latin-1")
        "é".encode(chain.name)
        self.assertEqual(seen, [b"\xe9"])

    def test_chain_output_is_decoded_as_text(self):
        chain = self.reg(decode=lambda data: b"\xe9", encoding="latin-1")
        self.assertEqual(b"anything".decode(chain.name), "é")


class ConsumedLengthTests(RegistryTestCase):
    def test_encode_returns_character_count(self):
        info = codecs.lookup(self.reg().name)
        for text, expected in [("", 0), ("abc", 3), ("héllo", 5), ("日本語", 3)]:
            with self.subTest(text=text):
                self.assertEqual(info.encode(text)[1], expected)

    def test_decode_returns_byte_count(self):
        info = codecs.lookup(self.reg().name)
        for data, expected in [(b"", 0), (b"abc", 3), (b"h\xc3\xa9llo", 6)]:
            with self.subTest(data=data):
                self.assertEqual(info.decode(data)[1], expected)

    def test_lengths_describe_input_not_chain_output(self):
        chain = self.reg(add_prefix, strip_prefix)
        info = codecs.lookup(chain.name)
        encoded, consumed = info.encode("ab")
        self.assertEqual((encoded, consumed), (b"xab", 2))  # prefix adds a byte
        text, consumed = info.decode(b"xab")
        self.assertEqual((text, consumed), ("ab", 3))  # prefix is consumed too

    def test_decode_accepts_buffer_types(self):
        info = codecs.lookup(self.reg(reverse_bytes, reverse_bytes).name)
        for wrap in (bytes, bytearray, memoryview):
            with self.subTest(wrap=wrap.__name__):
                self.assertEqual(info.decode(wrap(b"cba")), ("abc", 3))

    def test_bytearray_decode_method(self):
        chain = self.reg(reverse_bytes, reverse_bytes)
        self.assertEqual(bytearray(b"cba").decode(chain.name), "abc")


class ErrorsArgumentTests(RegistryTestCase):
    def test_encode_strict_raises(self):
        chain = self.reg(encoding="ascii")
        with self.assertRaises(UnicodeEncodeError):
            "é".encode(chain.name)

    def test_encode_error_handlers_are_forwarded(self):
        chain = self.reg(encoding="ascii")
        cases = [
            ("replace", b"?"),
            ("ignore", b""),
            ("xmlcharrefreplace", b"&#233;"),
            ("backslashreplace", b"\\xe9"),
        ]
        for handler, expected in cases:
            with self.subTest(handler=handler):
                self.assertEqual("é".encode(chain.name, handler), expected)

    def test_encode_errors_are_handled_before_the_chain(self):
        chain = self.reg(reverse_bytes, reverse_bytes, encoding="ascii")
        # "aé" -> b"a?" at the text layer, then reversed by the chain
        self.assertEqual("aé".encode(chain.name, "replace"), b"?a")

    def test_decode_strict_raises(self):
        chain = self.reg()
        with self.assertRaises(UnicodeDecodeError):
            b"\xff".decode(chain.name)

    def test_decode_error_handlers_are_forwarded(self):
        chain = self.reg()
        cases = [
            ("replace", "\ufffd"),
            ("ignore", ""),
            ("backslashreplace", "\\xff"),
        ]
        for handler, expected in cases:
            with self.subTest(handler=handler):
                self.assertEqual(b"\xff".decode(chain.name, handler), expected)


class ChainFailureTests(RegistryTestCase):
    def test_encode_failure_raises_codec_error(self):
        chain = self.reg(encode=boom)
        with self.assertRaises(CodecError) as cm:
            "abc".encode(chain.name)
        err = cm.exception
        self.assertEqual(
            (err.op, err.codec, err.chain), ("encode", chain.name, chain.name)
        )
        self.assertIsInstance(err.__cause__, ValueError)

    def test_decode_failure_raises_codec_error(self):
        chain = self.reg(decode=boom)
        with self.assertRaises(CodecError) as cm:
            b"abc".decode(chain.name)
        err = cm.exception
        self.assertEqual(
            (err.op, err.codec, err.chain), ("decode", chain.name, chain.name)
        )
        self.assertIsInstance(err.__cause__, ValueError)

    def test_cause_types_are_preserved(self):
        for exc_type in (ValueError, KeyError, TypeError, RuntimeError):
            with self.subTest(exc_type=exc_type.__name__):

                def raiser(_, exc_type=exc_type):
                    raise exc_type("x")

                chain = self.reg(encode=raiser)
                with self.assertRaises(CodecError) as cm:
                    "abc".encode(chain.name)
                self.assertIsInstance(cm.exception.__cause__, exc_type)

    def test_text_encode_error_means_chain_never_runs(self):
        chain = self.reg(encoding="ascii")
        with self.assertRaises(UnicodeEncodeError):
            "é".encode(chain.name)
        self.assertEqual(self.log, [])

    def test_text_decode_error_happens_after_chain_runs(self):
        chain = self.reg(decode=lambda data: b"\xff")
        with self.assertRaises(UnicodeDecodeError):
            b"abc".decode(chain.name)
        self.assertEqual(self.log, [f"decode:{chain.name}"])


if __name__ == "__main__":
    unittest.main()
