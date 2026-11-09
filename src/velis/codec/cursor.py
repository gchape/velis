"""Exact byte consumption and whole-record validation."""

from io import BytesIO
from typing import Self

from velis.errors import ProtocolError


class Cursor:
    """Read one encoded record through an owned BytesIO buffer.

    Used as a context manager, it requires the whole record to be consumed:
    leaving the block normally calls finish(). Exceptional exits preserve the
    original failure, even if bytes remain unread.
    """

    __slots__ = ("_buffer", "_length")

    def __init__(self, data: bytes) -> None:
        self._buffer = BytesIO(data)
        self._length = len(data)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, *_: object) -> None:
        if exc_type is None:
            self.finish()

    def take(self, size: int) -> bytes:
        """Consume exactly size bytes, preserving position on truncation.

        Negative sizes raise ValueError; insufficient data raises ProtocolError.
        Zero consumes nothing and returns empty bytes.
        """
        if size < 0:
            raise ValueError("read size must be nonnegative")
        if size > self._length - self._buffer.tell():
            raise ProtocolError(f"truncated record: need {size} bytes")
        return self._buffer.read(size)

    def take_uint(self, size: int) -> int:
        """Read a big-endian unsigned integer of any positive byte width."""
        if size <= 0:
            raise ValueError("integer length must be positive")
        return int.from_bytes(self.take(size))

    def finish(self) -> None:
        """Raise unless every byte of the record has been read."""
        if self._buffer.tell() != self._length:
            raise ProtocolError("trailing bytes after record")
