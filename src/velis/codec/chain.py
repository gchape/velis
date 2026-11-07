from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class Codec(Protocol):
    @property
    def name(self) -> str: ...

    def encode(self, data: bytes) -> bytes: ...
    def decode(self, data: bytes) -> bytes: ...


@dataclass(frozen=True, slots=True)
class CodecChain:
    codecs: tuple[Codec, ...] = ()

    @property
    def name(self) -> str:
        return "_".join(codec.name for codec in self.codecs)

    def encode(self, data: bytes) -> bytes:
        for codec in self.codecs:
            data = codec.encode(data)
        return data

    def decode(self, data: bytes) -> bytes:
        for codec in reversed(self.codecs):
            data = codec.decode(data)
        return data

    def __or__(self, codec: Codec) -> CodecChain:
        return CodecChain((*self.codecs, codec))

    def __ror__(self, codec: Codec) -> CodecChain:
        return CodecChain((codec, *self.codecs))
