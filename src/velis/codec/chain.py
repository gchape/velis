from dataclasses import dataclass
from typing import Any, Protocol, cast


class Codec[T, Encoded](Protocol):
    """A reversible representation of a value."""

    def encode(self, value: T, /) -> Encoded: ...

    def decode(self, data: Encoded, /) -> T: ...


def _steps(codec: Codec[Any, Any]) -> tuple[Codec[Any, Any], ...]:
    """Flatten a chain into its stages; wrap a plain codec as one stage."""
    return codec.codecs if isinstance(codec, CodecChain) else (codec,)


@dataclass(frozen=True, slots=True)
class CodecChain[T, Encoded]:
    """Compose representations; encode in order, decode in reverse.

    Start with ``of(codec)`` and append with ``|``.
    Type checkers verify that each stage's output matches the next stage's
    input. At runtime the stages are stored untyped, so a mismatch only
    shows up when encoding or decoding.
    """

    codecs: tuple[Codec[Any, Any], ...] = ()

    @staticmethod
    def of[A, B](codec: Codec[A, B]) -> CodecChain[A, B]:
        return CodecChain(_steps(codec))

    def encode(self, value: T) -> Encoded:
        current: Any = value
        for codec in self.codecs:
            current = codec.encode(current)
        return cast(Encoded, current)

    def decode(self, data: Encoded) -> T:
        current: Any = data
        for codec in reversed(self.codecs):
            current = codec.decode(current)
        return cast(T, current)

    def __or__[Next](self, other: Codec[Encoded, Next]) -> CodecChain[T, Next]:
        return CodecChain(self.codecs + _steps(other))
