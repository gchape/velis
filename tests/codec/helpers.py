class Prefix:
    """Test codec that prepends a byte and removes it during decoding."""

    def encode(self, data: bytes) -> bytes:
        return b"x" + data

    def decode(self, data: bytes) -> bytes:
        return data[1:]


class Reverse:
    """Test codec that reverses bytes in both directions."""

    def encode(self, data: bytes) -> bytes:
        return data[::-1]

    def decode(self, data: bytes) -> bytes:
        return data[::-1]
