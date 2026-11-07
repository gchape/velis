from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class Codec(Protocol):
    name: str

    def encode(self, data: bytes) -> bytes: ...
    def decode(self, data: bytes) -> bytes: ...


@dataclass(frozen=True, slots=True)
class CodecChain:
    codecs: tuple[Codec, ...] = ()

    @property
    def name(self) -> str:
        return "_".join(c.name for c in self.codecs)

    def encode(self, data: bytes) -> bytes:
        for codec in self.codecs:
            data = codec.encode(data)
        return data

    def decode(self, data: bytes) -> bytes:
        for codec in reversed(self.codecs):
            data = codec.decode(data)
        return data
