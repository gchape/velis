class Prefix:
    """Test codec that prepends a byte and removes it during decoding."""

    def __init__(self, name: str = "testprefix") -> None:
        self.name = name

    def encode(self, data: bytes) -> bytes:
        return b"x" + data

    def decode(self, data: bytes) -> bytes:
        return data[1:]


class Reverse:
    """Test codec that reverses bytes in both directions."""

    name = "testreverse"

    def encode(self, data: bytes) -> bytes:
        return data[::-1]

    def decode(self, data: bytes) -> bytes:
        return data[::-1]
