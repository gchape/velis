from __future__ import annotations

import codecs
import logging
import re
from collections.abc import Buffer

from velis.codec.chain import CodecChain

log = logging.getLogger(__name__)

_CODEC_NAME = re.compile(r"[a-z0-9]+(_[a-z0-9]+)*")
_registered_codecs: dict[str, codecs.CodecInfo] = {}


def _lookup_codec(name: str) -> codecs.CodecInfo | None:
    """Look up chains registered through this module's Python codec hook."""
    return _registered_codecs.get(name)


def _validate_name(name: str) -> None:
    """Reject invalid names and names already present in the codec registry."""
    if not _CODEC_NAME.fullmatch(name):
        raise ValueError(f"invalid codec name (want e.g. 'gzip_b64'): {name!r}")
    try:
        codecs.lookup(name)
    except LookupError:
        return
    raise ValueError(f"codec name already in use: {name!r}")


def _make_text_codec(
    chain: CodecChain[bytes, bytes], encoding: str
) -> codecs.CodecInfo:
    """Adapt a byte codec chain to Python's text and consumed-length interface."""

    def encode(text: str, errors: str = "strict") -> tuple[bytes, int]:
        return chain.encode(text.encode(encoding, errors)), len(text)

    def decode(data: Buffer, errors: str = "strict") -> tuple[str, int]:
        raw = bytes(data)
        return chain.decode(raw).decode(encoding, errors), len(raw)

    return codecs.CodecInfo(encode, decode, name=chain.name)


def register(
    chain: CodecChain[bytes, bytes], encoding: str = "utf-8"
) -> CodecChain[bytes, bytes]:
    """Register a chain as a Python text codec and return the same chain.

    Encode text before applying the chain, and decode text after reversing
    the chain. Reject empty, invalid or already registered names.
    """
    name = chain.name
    _validate_name(name)
    text_codec = _make_text_codec(chain, encoding)
    if not _registered_codecs:
        codecs.register(_lookup_codec)
    _registered_codecs[name] = text_codec
    log.info("registered codec %r (text encoding %s)", name, encoding)
    return chain


def unregister(name: str) -> None:
    """Remove a registered chain and clear cached Python codec lookups."""
    del _registered_codecs[name]
    codecs.unregister(_lookup_codec)
    if _registered_codecs:
        codecs.register(_lookup_codec)
