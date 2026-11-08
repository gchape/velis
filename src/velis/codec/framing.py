"""Sans-I/O length-prefixed framing: bytes in, complete bodies out."""

from collections.abc import Iterator

from velis.codec.binary import U32, LengthPrefix


class FrameDecoder:
    """Split a byte stream into frame bodies without owning any I/O.

    Feed whatever the transport delivers, then iterate ``bodies()``. Each
    header is validated against the length limit before waiting for more body
    bytes. Transports should bound the chunks passed to ``feed()``.
    """

    def __init__(self, framing: LengthPrefix) -> None:
        self._framing = framing
        self._buffer = bytearray()
        self._pos = 0

    @property
    def pending(self) -> bytes:
        """Bytes received that do not yet form a complete frame."""
        return bytes(self._buffer[self._pos :])

    def feed(self, data: bytes) -> None:
        self._buffer += data

    def bodies(self) -> Iterator[bytes]:
        """Yield each complete body, raising ProtocolError on a bad header."""
        header = U32.width
        while len(self._buffer) - self._pos >= header:
            start = self._pos + header
            length = self._framing.decode_length(bytes(self._buffer[self._pos : start]))
            end = start + length
            if len(self._buffer) < end:
                break
            self._pos = end
            yield bytes(self._buffer[start:end])
        del self._buffer[: self._pos]
        self._pos = 0
