"""Encode a closed set of dataclass variants as a text tag plus ordered fields."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from typing import Any

from velis.codec.binary import Utf8
from velis.codec.cursor import Cursor
from velis.codec.fields import Field, Prefixed
from velis.errors import ProtocolError

TAG = Prefixed(Utf8(), 32)


@dataclass(frozen=True, slots=True)
class _Variant:
    tag: bytes
    cls: type[Any]
    names: tuple[str, ...]
    codecs: tuple[Field[Any], ...]


class TaggedUnion[T]:
    """One table drives both directions, so encode and decode cannot drift.

    Each variant is a dataclass; its fields are encoded in declaration order
    with the field at the same position. Each field explicitly owns its fixed
    width or length prefix, independently of the codec transforming its value.
    """

    def __init__(
        self,
        what: str,
        variants: Mapping[str, tuple[type[T], Sequence[Field[Any]]]],
    ) -> None:
        self._what = what
        self._by_tag: dict[str, _Variant] = {}
        self._by_type: dict[type[Any], _Variant] = {}
        for tag, (cls, codecs) in variants.items():
            if not is_dataclass(cls):
                raise TypeError(f"{cls.__name__} must be a dataclass")
            members = fields(cls)
            if any(not member.init or member.kw_only for member in members):
                raise ValueError(
                    f"{cls.__name__} fields must be positional init fields"
                )
            names = tuple(member.name for member in members)
            if len(names) != len(codecs):
                raise ValueError(
                    f"{cls.__name__} has {len(names)} fields but {len(codecs)} codecs"
                )
            if cls in self._by_type:
                raise ValueError(f"{cls.__name__} is listed twice")
            try:
                encoded_tag = TAG.encode(tag)
            except ProtocolError as exc:
                raise ValueError(f"invalid tag: {tag!r}") from exc
            variant = _Variant(encoded_tag, cls, names, tuple(codecs))
            self._by_tag[tag] = variant
            self._by_type[cls] = variant

    @property
    def types(self) -> frozenset[type[Any]]:
        """The variant classes this union can encode."""
        return frozenset(self._by_type)

    def encode(self, value: T) -> bytes:
        variant = self._by_type.get(type(value))
        if variant is None:
            raise ProtocolError(f"unsupported {self._what}: {type(value).__name__}")
        body = (
            codec.encode(getattr(value, name))
            for codec, name in zip(variant.codecs, variant.names, strict=True)
        )
        return variant.tag + b"".join(body)

    def decode(self, data: bytes) -> T:
        cursor = Cursor(data)
        tag = TAG.read(cursor)
        variant = self._by_tag.get(tag)
        if variant is None:
            raise ProtocolError(f"unknown {self._what}: {tag[:40]!r}")
        value = variant.cls(*(codec.read(cursor) for codec in variant.codecs))
        cursor.finish()
        return value
