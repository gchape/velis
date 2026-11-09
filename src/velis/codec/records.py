"""Positional records and tagged choices composed from field codecs."""

from annotationlib import Format
from collections.abc import Mapping
from inspect import Parameter, signature
from typing import Any

from velis.codec.cursor import Cursor
from velis.codec.fields import CursorCodec, Field, LengthPrefixedField
from velis.codec.values import U32, UTF8
from velis.errors import ProtocolError

_POSITIONAL = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
TAG = LengthPrefixedField(UTF8(), 32, prefix=U32)


class Record[T](CursorCodec[T]):
    """Encode constructor fields in order, one codec per positional parameter.

    Each parameter must correspond to a readable attribute of the same name.
    Keyword-only and variadic parameters are rejected during setup. Dataclass
    fields with init=False are derived state, so they are not encoded.
    Explicit field codecs define the layout independently of type annotations.
    """

    def __init__(self, cls: type[T], *codecs: Field[Any]) -> None:
        parameters = signature(cls, annotation_format=Format.STRING).parameters.values()
        if any(p.kind not in _POSITIONAL for p in parameters):
            raise ValueError(f"{cls.__name__} fields must be positional init fields")
        names = tuple(p.name for p in parameters)
        if len(names) != len(codecs):
            raise ValueError(
                f"{cls.__name__} has {len(names)} fields but {len(codecs)} codecs"
            )
        self.cls = cls
        self._parts = tuple(zip(names, codecs, strict=True))

    def encode(self, value: T) -> bytes:
        return b"".join(
            codec.encode(getattr(value, name)) for name, codec in self._parts
        )

    def read(self, cursor: Cursor) -> T:
        return self.cls(*(codec.read(cursor) for _, codec in self._parts))


class Choice[T](CursorCodec[T]):
    """Choose a Record by a text tag; one table drives both directions."""

    def __init__(self, kind: str, variants: Mapping[str, Record[Any]]) -> None:
        self._kind = kind
        self._by_tag = dict(variants)
        self._by_type: dict[type[Any], tuple[bytes, Record[Any]]] = {}
        for tag, record in self._by_tag.items():
            if record.cls in self._by_type:
                raise ValueError(f"{record.cls.__name__} is listed twice")
            try:
                encoded_tag = TAG.encode(tag)
            except ProtocolError as exc:
                raise ValueError(f"invalid tag: {tag!r}") from exc
            self._by_type[record.cls] = encoded_tag, record

    @property
    def types(self) -> frozenset[type[Any]]:
        """The variant classes this choice can encode."""
        return frozenset(self._by_type)

    def encode(self, value: T) -> bytes:
        if (entry := self._by_type.get(type(value))) is None:
            raise ProtocolError(f"unsupported {self._kind}: {type(value).__name__}")
        tag, record = entry
        return tag + record.encode(value)

    def read(self, cursor: Cursor) -> T:
        tag = TAG.read(cursor)
        if (record := self._by_tag.get(tag)) is None:
            raise ProtocolError(f"unknown {self._kind}: {tag!r}")
        return record.read(cursor)
