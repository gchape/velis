from __future__ import annotations

import codecs
import logging
from collections.abc import Buffer

from velis.codec.chain import CodecChain

logger = logging.getLogger(__name__)

_infos: dict[str, codecs.CodecInfo] = {}


def register(chain: CodecChain, encoding: str = "utf-8") -> CodecChain:
    if not chain.codecs:
        raise ValueError("cannot register an empty chain")
    if chain.name in _infos:
        raise ValueError(f"codec already registered: {chain.name!r}")

    def encode(text: str, errors: str = "strict") -> tuple[bytes, int]:
        return chain.encode(text.encode(encoding, errors)), len(text)

    def decode(data: Buffer, errors: str = "strict") -> tuple[str, int]:
        raw = bytes(data)
        return chain.decode(raw).decode(encoding, errors), len(raw)

    _infos[chain.name] = codecs.CodecInfo(encode, decode, name=chain.name)
    logger.info("registered codec %r (text encoding %s)", chain.name, encoding)
    return chain


codecs.register(_infos.get)
