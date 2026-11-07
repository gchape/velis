import codecs
import itertools
import subprocess
import sys
import textwrap
import unittest

from tests.codec.helpers import Prefix, Reverse
from velis.codec import CodecChain, register, unregister

_counter = itertools.count()


class RegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.names: set[str] = set()
        self.addCleanup(self.unregister_all)

    def unregister_all(self) -> None:
        for name in self.names:
            unregister(name)

    def register_chain(self, encoding: str = "utf-8") -> CodecChain:
        chain = CodecChain.of(Prefix(f"velisregistrytest{next(_counter)}"), Reverse())
        self.assertIs(register(chain, encoding), chain)
        self.names.add(chain.name)
        return chain

    def test_text_roundtrip_and_transform_order(self) -> None:
        chain = self.register_chain()
        for text in ("", "hello", "héllo", "ქართული", "🙂", "a\x00b"):
            with self.subTest(text=text):
                encoded = text.encode(chain.name)
                self.assertEqual(encoded, (b"x" + text.encode())[::-1])
                self.assertEqual(encoded.decode(chain.name), text)

    def test_codecs_module_roundtrip(self) -> None:
        chain = self.register_chain()
        self.assertEqual(codecs.encode("ab", chain.name), b"bax")
        self.assertEqual(codecs.decode(b"bax", chain.name), "ab")

    def test_custom_text_encoding(self) -> None:
        chain = self.register_chain("latin-1")
        self.assertEqual("é".encode(chain.name), b"\xe9x")
        self.assertEqual(b"\xe9x".decode(chain.name), "é")

    def test_consumed_lengths_describe_input(self) -> None:
        info = codecs.lookup(self.register_chain().name)
        self.assertEqual(info.encode("é"), (b"\xa9\xc3x", 1))
        self.assertEqual(info.decode(b"\xa9\xc3x"), ("é", 3))

    def test_decode_accepts_buffers(self) -> None:
        info = codecs.lookup(self.register_chain().name)
        for wrap in (bytes, bytearray, memoryview):
            with self.subTest(buffer=wrap.__name__):
                self.assertEqual(info.decode(wrap(b"bax")), ("ab", 3))

    def test_encoding_errors_forwarded_before_transform(self) -> None:
        name = self.register_chain("ascii").name
        with self.assertRaises(UnicodeEncodeError):
            "aé".encode(name)
        self.assertEqual("aé".encode(name, "replace"), b"?ax")
        self.assertEqual("aé".encode(name, "ignore"), b"ax")

    def test_decoding_errors_forwarded_after_transform(self) -> None:
        name = self.register_chain().name
        with self.assertRaises(UnicodeDecodeError):
            b"\xffx".decode(name)
        self.assertEqual(b"\xffx".decode(name, "replace"), "\ufffd")
        self.assertEqual(b"\xffx".decode(name, "ignore"), "")

    def test_invalid_names_are_rejected(self) -> None:
        names = ("", "Uppercase", "has space", "has-dash", "_start", "end_", "a__b")
        for name in names:
            with self.subTest(name=name), self.assertRaises(ValueError):
                register(CodecChain.of(Prefix(name)))
        with self.assertRaises(ValueError):
            register(CodecChain())

    def test_builtin_names_are_rejected(self) -> None:
        for name in ("utf8", "ascii", "base64", "zlib"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                register(CodecChain.of(Prefix(name)))
        self.assertEqual(codecs.encode("é", "utf8"), b"\xc3\xa9")

    def test_duplicate_registration_preserves_original(self) -> None:
        chain = self.register_chain()
        with self.assertRaises(ValueError):
            register(CodecChain.of(Prefix(chain.name)))
        self.assertEqual("ab".encode(chain.name), b"bax")

    def test_unregister_clears_lookup_cache_and_allows_replacement(self) -> None:
        chain = self.register_chain()
        name = chain.name
        self.assertEqual("ab".encode(name), b"bax")
        unregister(name)
        self.names.remove(name)
        with self.assertRaises(LookupError):
            codecs.lookup(name)
        register(CodecChain.of(Prefix(name)))
        self.names.add(name)
        self.assertEqual("ab".encode(name), b"xab")
        self.assertEqual(b"xab".decode(name), "ab")

    def test_unregister_preserves_other_registered_codecs(self) -> None:
        first, second = self.register_chain(), self.register_chain()
        codecs.lookup(first.name)
        codecs.lookup(second.name)
        unregister(first.name)
        self.names.remove(first.name)
        with self.assertRaises(LookupError):
            codecs.lookup(first.name)
        self.assertEqual("ab".encode(second.name), b"bax")
        self.assertEqual(codecs.encode("ab", "utf8"), b"ab")

    def test_import_defers_global_registration_until_first_register(self) -> None:
        """Check import behavior in a fresh interpreter with an empty registry."""
        script = textwrap.dedent(
            """
            import codecs
            from unittest.mock import patch
            from tests.codec.helpers import Prefix

            with patch.object(codecs, "register", wraps=codecs.register) as hook:
                from velis.codec import CodecChain, register, unregister
                hook.assert_not_called()
                chain = register(CodecChain.of(Prefix("lazytest")))
                hook.assert_called_once()
                assert "ab".encode(chain.name) == b"xab"
                unregister(chain.name)
                try:
                    codecs.lookup(chain.name)
                except LookupError:
                    pass
                else:
                    raise AssertionError("unregistered codec is still visible")
                hook.assert_called_once()
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, check=False
        )
        self.assertEqual(result.returncode, 0, result.stderr)
