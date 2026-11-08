from typing import Self

from velis.errors import ProtocolError


class Cursor:
    """Sequential reader over one encoded record.

    Used as a context manager, it requires the whole record to be consumed:
    leaving the block normally calls finish().
    """

    __slots__ = ("_data", "_offset")

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, *_: object) -> None:
        if exc_type is None:
            self.finish()

    def take(self, size: int) -> bytes:
        """Consume exactly size bytes, preserving position on truncation."""
        if size < 0:
            raise ValueError("read size must be nonnegative")
        end = self._offset + size
        if end > len(self._data):
            raise ProtocolError(f"truncated record: need {size} bytes")
        chunk = self._data[self._offset : end]
        self._offset = end
        return chunk

    def finish(self) -> None:
        """Raise unless every byte of the record has been read."""
        if self._offset != len(self._data):
            raise ProtocolError("trailing bytes after record")
