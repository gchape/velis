from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, cast


class Codec[T, Encoded](Protocol):
    """A named, reversible representation of a value."""

    @property
    def name(self) -> str: ...

    def encode(self, value: T, /) -> Encoded: ...

    def decode(self, data: Encoded, /) -> T: ...


@dataclass(frozen=True, slots=True, init=False)
class CodecChain[T, Encoded]:
    """Compose compatible representations; decode in reverse order.

    Start with ``of(codec)`` or ``identity(type)`` and append with ``|``.
    Intermediate types are erased only in storage; composition checks that
    each stage's output matches the following stage's input.
    """

    codecs: tuple[Codec[Any, Any], ...]

    def __init__(self, codec: Codec[T, Encoded]) -> None:
        steps = codec.codecs if isinstance(codec, CodecChain) else (codec,)
        object.__setattr__(self, "codecs", steps)

    @staticmethod
    def of[A, B](codec: Codec[A, B]) -> CodecChain[A, B]:
        return CodecChain(codec)

    @staticmethod
    def identity[A](value_type: type[A]) -> CodecChain[A, A]:
        """Create an empty chain with explicit, identical endpoint types."""
        return CodecChain._from_steps(())

    @classmethod
    def _from_steps(cls, steps: tuple[Codec[Any, Any], ...]) -> CodecChain[T, Encoded]:
        chain = object.__new__(cls)
        object.__setattr__(chain, "codecs", steps)
        return chain

    @property
    def name(self) -> str:
        return "_".join(codec.name for codec in self.codecs)

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
        steps = other.codecs if isinstance(other, CodecChain) else (other,)
        return CodecChain._from_steps(self.codecs + steps)

    def __ror__[Previous](
        self, other: Codec[Previous, T]
    ) -> CodecChain[Previous, Encoded]:
        return CodecChain._from_steps((other, *self.codecs))
