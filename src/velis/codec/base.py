"""Reversible components and their ordered, recursive composition."""

from dataclasses import dataclass
from typing import Any, Protocol, cast


class Codec[T, Encoded](Protocol):
    """A reversible representation of a value."""

    def encode(self, value: T, /) -> Encoded: ...

    def decode(self, data: Encoded, /) -> T: ...


def _steps(codec: Codec[Any, Any]) -> tuple[Codec[Any, Any], ...]:
    return codec.codecs if type(codec) is CompositeCodec else (codec,)


@dataclass(frozen=True, slots=True)
class CompositeCodec[T, Encoded]:
    """Aggregate child codecs: encode in order and decode in reverse.

    Children can be individual codecs or other composites. Direct construction
    preserves nested groups. The typed ``of(first) | next`` shorthand
    concatenates stages at the top level.
    Subclasses remain opaque stages, preserving overridden encode/decode methods.
    Adjacent representations must agree; direct construction does not check
    those types at runtime. An empty composite returns its input unchanged.
    """

    codecs: tuple[Codec[Any, Any], ...] = ()

    @staticmethod
    def of[A, B](codec: Codec[A, B]) -> CompositeCodec[A, B]:
        return CompositeCodec(_steps(codec))

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

    def __or__[Next](self, other: Codec[Encoded, Next]) -> CompositeCodec[T, Next]:
        return CompositeCodec(self.codecs + _steps(other))
