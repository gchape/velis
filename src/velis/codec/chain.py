from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Protocol

from velis.exceptions import CodecError


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

    @contextmanager
    def _wrap_errors(self, op: str, codec: Codec) -> Generator[None]:
        try:
            yield
        except CodecError:
            raise
        except Exception as exc:
            raise CodecError(op, codec.name, self.name) from exc

    def encode(self, data: bytes) -> bytes:
        """Encode data through every codec in order.

        Raises:
            CodecError: if any codec fails.
        """
        for codec in self.codecs:
            with self._wrap_errors("encode", codec):
                data = codec.encode(data)
        return data

    def decode(self, data: bytes) -> bytes:
        """Decode data through every codec in reverse order.

        Raises:
            CodecError: if any codec fails.
        """
        for codec in reversed(self.codecs):
            with self._wrap_errors("decode", codec):
                data = codec.decode(data)
        return data

    def __or__(self, codec: Codec) -> CodecChain:
        return CodecChain((*self.codecs, codec))

    def __ror__(self, codec: Codec) -> CodecChain:
        return CodecChain((codec, *self.codecs))
