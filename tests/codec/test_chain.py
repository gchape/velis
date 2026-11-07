import unittest

from velis.codec import CodecChain


class Fn:
    """Minimal codec built from two functions."""

    def __init__(self, name, encode=lambda d: d, decode=lambda d: d):
        self.name = name
        self._encode = encode
        self._decode = decode

    def encode(self, data):
        return self._encode(data)

    def decode(self, data):
        return self._decode(data)


def boom(data):
    raise ValueError("boom")


prefix = Fn("p", lambda d: b"x" + d, lambda d: d[1:])
reverse = Fn("r", lambda d: d[::-1], lambda d: d[::-1])


class ChainTests(unittest.TestCase):
    def test_name(self):
        self.assertEqual(CodecChain().name, "")
        self.assertEqual(CodecChain((prefix, reverse)).name, "p_r")

    def test_empty_chain_is_identity(self):
        self.assertEqual(CodecChain().encode(b"ab"), b"ab")
        self.assertEqual(CodecChain().decode(b"ab"), b"ab")

    def test_encode_runs_codecs_in_order(self):
        # prefix -> b"xab", then reverse -> b"bax"
        self.assertEqual(CodecChain((prefix, reverse)).encode(b"ab"), b"bax")

    def test_decode_runs_codecs_in_reverse_order(self):
        # reverse -> b"xab", then strip prefix -> b"ab"
        self.assertEqual(CodecChain((prefix, reverse)).decode(b"bax"), b"ab")

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

    def test_or_appends_and_ror_prepends(self):
        self.assertEqual((CodecChain((prefix,)) | reverse).codecs, (prefix, reverse))
        self.assertEqual((prefix | CodecChain((reverse,))).codecs, (prefix, reverse))

    def test_or_does_not_mutate(self):
        original = CodecChain((prefix,))
        original | reverse
        self.assertEqual(original.codecs, (prefix,))


if __name__ == "__main__":
    unittest.main()
