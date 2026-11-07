from __future__ import annotations

import codecs
import logging
import re
from collections.abc import Buffer

from velis.codec.chain import CodecChain

log = logging.getLogger(__name__)

_NAME = re.compile(r"[a-z0-9]+(_[a-z0-9]+)*")
_infos: dict[str, codecs.CodecInfo] = {}


def _validate(name: str) -> None:
    if not _NAME.fullmatch(name):
        raise ValueError(f"invalid codec name (want e.g. 'gzip_b64'): {name!r}")
    try:
        codecs.lookup(name)
    except LookupError:
        return
    raise ValueError(f"codec name already in use: {name!r}")


def register(chain: CodecChain, encoding: str = "utf-8") -> CodecChain:
    name = chain.name
    _validate(name)

    def encode(text: str, errors: str = "strict") -> tuple[bytes, int]:
        return chain.encode(text.encode(encoding, errors)), len(text)

    def decode(data: Buffer, errors: str = "strict") -> tuple[str, int]:
        return chain.decode(data := bytes(data)).decode(encoding, errors), len(data)

    _infos[name] = codecs.CodecInfo(encode, decode, name=name)
    log.info("registered codec %r (text encoding %s)", name, encoding)
    return chain


codecs.register(_infos.get)
