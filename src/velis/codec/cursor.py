from dataclasses import dataclass, field

from velis.errors import ProtocolError


@dataclass(slots=True, eq=False)
class Cursor:
    """Sequential reader over one encoded record."""

    _data: bytes = field(repr=False)
    _offset: int = field(default=0, init=False)

    def take(self, size: int) -> bytes:
        """Consume exactly size bytes, preserving position on truncation."""
        if size < 0:
            raise ValueError("read size must be nonnegative")
        end = self._offset + size
        if end > len(self._data):
            raise ProtocolError("truncated record")
        chunk = self._data[self._offset : end]
        self._offset = end
        return chunk

    def finish(self) -> None:
        """Raise unless every byte of the record has been read."""
        if self._offset != len(self._data):
            raise ProtocolError("trailing bytes after record")
