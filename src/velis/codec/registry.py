from __future__ import annotations

import codecs
import logging
import re
from collections.abc import Buffer

from velis.codec.chain import CodecChain

logger = logging.getLogger(__name__)

_NAME = re.compile(r"[a-z0-9]+(_[a-z0-9]+)*")
_infos: dict[str, codecs.CodecInfo] = {}


def _is_known(name: str) -> bool:
    try:
        codecs.lookup(name)
    except LookupError:
        return False
    return True


def register(chain: CodecChain, encoding: str = "utf-8") -> CodecChain:
    name = chain.name
    if not _NAME.fullmatch(name):
        raise ValueError(f"invalid codec name (want e.g. 'gzip_b64'): {name!r}")
    if _is_known(name):
        raise ValueError(f"codec name already in use: {name!r}")

    def encode(text: str, errors: str = "strict") -> tuple[bytes, int]:
        return chain.encode(text.encode(encoding, errors)), len(text)

    def decode(data: Buffer, errors: str = "strict") -> tuple[str, int]:
        return chain.decode(data := bytes(data)).decode(encoding, errors), len(data)

    _infos[name] = codecs.CodecInfo(encode, decode, name=name)
    logger.info("registered codec %r (text encoding %s)", name, encoding)
    return chain


codecs.register(_infos.get)
