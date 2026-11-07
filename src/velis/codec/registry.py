from __future__ import annotations

import codecs
import logging
import re
from collections.abc import Buffer

from velis.codec.chain import CodecChain

log = logging.getLogger(__name__)

_NAME = re.compile(r"[a-z0-9]+(_[a-z0-9]+)*")
_infos: dict[str, codecs.CodecInfo] = {}


def _search(name: str) -> codecs.CodecInfo | None:
    return _infos.get(name)


def _validate(name: str) -> None:
    """Reject invalid names and names already present in the codec registry."""
    if not _NAME.fullmatch(name):
        raise ValueError(f"invalid codec name (want e.g. 'gzip_b64'): {name!r}")
    try:
        codecs.lookup(name)
    except LookupError:
        return
    raise ValueError(f"codec name already in use: {name!r}")


def register(chain: CodecChain, encoding: str = "utf-8") -> CodecChain:
    """Register a chain as a Python text codec and return the same chain.

    Encode text before applying the chain, and decode text after reversing
    the chain. Reject empty, invalid or already registered names.
    """
    name = chain.name
    _validate(name)

    def encode(text: str, errors: str = "strict") -> tuple[bytes, int]:
        return chain.encode(text.encode(encoding, errors)), len(text)

    def decode(data: Buffer, errors: str = "strict") -> tuple[str, int]:
        raw = bytes(data)
        return chain.decode(raw).decode(encoding, errors), len(raw)

    _infos[name] = codecs.CodecInfo(encode, decode, name=name)
    log.info("registered codec %r (text encoding %s)", name, encoding)
    return chain


def unregister(name: str) -> None:
    """Remove a registered chain and clear cached Python codec lookups."""
    del _infos[name]
    codecs.unregister(_search)
    codecs.register(_search)


codecs.register(_search)
