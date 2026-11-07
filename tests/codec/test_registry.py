import codecs
import itertools
import unittest

from velis.codec import CodecChain, register

# The codec registry is global, so every test registers under a fresh name.
_counter = itertools.count()


class Fn:
    def __init__(self, name, fn=lambda d: d):
        self.name = name
        self._fn = fn

    def encode(self, data):
        return self._fn(data)

    def decode(self, data):
        return self._fn(data)


def new_chain(fn=lambda d: d):
    return CodecChain((Fn(f"registrytest{next(_counter)}", fn),))


class RegisterTests(unittest.TestCase):
    def test_returns_chain_and_makes_it_lookup_able(self):
        chain = new_chain()
        self.assertIs(register(chain), chain)
        self.assertEqual(codecs.lookup(chain.name).name, chain.name)

    def test_empty_chain_rejected(self):
        with self.assertRaises(ValueError):
            register(CodecChain())

    def test_duplicate_name_rejected(self):
        chain = register(new_chain())
        with self.assertRaises(ValueError):
            register(chain)


class RoundtripTests(unittest.TestCase):
    def test_str_and_bytes_roundtrip(self):
        chain = register(new_chain(lambda d: d[::-1]))
        encoded = "héllo".encode(chain.name)
        self.assertEqual(encoded, "héllo".encode()[::-1])
        self.assertEqual(encoded.decode(chain.name), "héllo")

    def test_custom_text_encoding(self):
        chain = register(new_chain(), encoding="latin-1")
        self.assertEqual("é".encode(chain.name), b"\xe9")
        self.assertEqual(b"\xe9".decode(chain.name), "é")

    def test_errors_argument_is_forwarded(self):
        chain = register(new_chain(), encoding="ascii")
        with self.assertRaises(UnicodeEncodeError):
            "é".encode(chain.name)
        self.assertEqual("é".encode(chain.name, "replace"), b"?")

    def test_consumed_lengths(self):
        chain = register(new_chain())
        info = codecs.lookup(chain.name)
        self.assertEqual(info.encode("héllo")[1], 5)  # characters
        self.assertEqual(info.decode(b"h\xc3\xa9llo")[1], 6)  # bytes


if __name__ == "__main__":
    unittest.main()
