"""The shared key-value interface and its plain in-memory base layer."""

from typing import Protocol


class KV(Protocol):
    """Interface every layer implements, so layers can wrap each other."""

    def get(self, key: str) -> bytes | None: ...
    def set(self, key: str, value: bytes) -> None: ...
    def delete(self, key: str) -> bool: ...
    def __contains__(self, key: object) -> bool: ...
    def __len__(self) -> int: ...
    @property
    def stored_bytes(self) -> int: ...


def entry_size(key: str, value: bytes) -> int:
    """UTF-8 key plus value bytes, the unit the byte budget counts."""
    return len(key.encode()) + len(value)


class Memory:
    """Dict plus byte accounting. Knows nothing about policies or TTLs."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}
        self._bytes = 0

    def get(self, key: str) -> bytes | None:
        return self._data.get(key)

    def set(self, key: str, value: bytes) -> None:
        self.delete(key)
        self._data[key] = value
        self._bytes += entry_size(key, value)

    def delete(self, key: str) -> bool:
        if (value := self._data.pop(key, None)) is None:
            return False
        self._bytes -= entry_size(key, value)
        return True

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)

    @property
    def stored_bytes(self) -> int:
        return self._bytes
