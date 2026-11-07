from collections import deque
from typing import Protocol, final


class Codec(Protocol):
    """A reversible bytes-to-bytes transformation: decode(encode(x)) == x."""

    @property
    def name(self) -> str: ...

    def encode(self, data: bytes) -> bytes: ...
    def decode(self, data: bytes) -> bytes: ...


@final
class CodecChain:
    """An ordered stack of bytes-to-bytes codecs with a text codec at the edge.

    Encoding: str -> bytes, then codecs first to last.
    Decoding: codecs last to first, then bytes -> str.
    """

    def __init__(self, encoding: str = "utf-8") -> None:
        self._encoding = encoding
        self._codecs: deque[Codec] = deque()

    @property
    def name(self) -> str:
        """Codec names joined with '_', e.g. 'zlib_base64'."""
        return "_".join(codec.name for codec in self._codecs)

    def add_first(self, codec: Codec) -> None:
        self._codecs.appendleft(codec)

    def add_last(self, codec: Codec) -> None:
        self._codecs.append(codec)

    def encode(self, text: str) -> bytes:
        data = text.encode(self._encoding)
        for codec in self._codecs:
            data = codec.encode(data)
        return data

    def decode(self, data: bytes) -> str:
        for codec in reversed(self._codecs):
            data = codec.decode(data)
        return data.decode(self._encoding)
