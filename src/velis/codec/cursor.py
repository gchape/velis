from dataclasses import dataclass, field

from velis.codec.binary import U32, UInt
from velis.codec.chain import Codec
from velis.errors import ProtocolError


@dataclass(slots=True, eq=False)
class Cursor:
    """Sequential reader over one encoded record."""

    _data: bytes = field(repr=False)
    _offset: int = field(default=0, init=False)

    def _take(self, size: int) -> bytes:
        end = self._offset + size
        if end > len(self._data):
            raise ProtocolError("truncated record")
        chunk = self._data[self._offset : end]
        self._offset = end
        return chunk

    def read[T](self, codec: Codec[T, bytes]) -> T:
        """Read one length-prefixed field and decode it with the codec."""
        header = self._take(U32.width)
        return codec.decode(header + self._take(U32.decode(header)))

    def read_uint(self, codec: UInt) -> int:
        """Read one fixed-width integer."""
        return codec.decode(self._take(codec.width))

    def finish(self) -> None:
        """Raise unless every byte of the record has been read."""
        if self._offset != len(self._data):
            raise ProtocolError("trailing bytes after record")
