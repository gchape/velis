from __future__ import annotations

import codecs
from collections.abc import Buffer

from velis.codec.chain import CodecChain

_infos: dict[str, codecs.CodecInfo] = {}


def register(chain: CodecChain, encoding: str = "utf-8") -> CodecChain:
    if not chain.codecs:
        raise ValueError("cannot register an empty chain")
    if chain.name in _infos:
        raise ValueError(f"codec already registered: {chain.name!r}")

    def encode(input: str, errors: str = "strict") -> tuple[bytes, int]:
        data = input.encode(encoding, errors)
        return chain.encode(data), len(input)

    def decode(input: Buffer, errors: str = "strict") -> tuple[str, int]:
        data = bytes(input)
        return chain.decode(data).decode(encoding, errors), len(data)

    _infos[chain.name] = codecs.CodecInfo(encode, decode, name=chain.name)
    return chain


codecs.register(_infos.get)
