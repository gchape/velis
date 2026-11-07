from __future__ import annotations


class CodecError(Exception):
    """A codec in a chain failed to encode or decode."""

    def __init__(self, op: str, codec: str, chain: str) -> None:
        super().__init__(op, codec, chain)
        self.op = op
        self.codec = codec
        self.chain = chain

    def __str__(self) -> str:
        return f"{self.op} failed in codec {self.codec!r} of chain {self.chain!r}"
