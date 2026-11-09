"""Shared checks for numeric limits and encoded byte lengths."""

from velis.errors import ProtocolError


def require_positive(**settings: int) -> None:
    """Raise ValueError for the first zero or negative setting, naming it."""
    for name, value in settings.items():
        if value <= 0:
            raise ValueError(f"invalid {name}: {value}")


def require_length(data: bytes, length: int, *, what: str) -> None:
    """Require exactly length bytes, raising ProtocolError on a mismatch.

    Both truncated and oversized data fail. The caller supplies the field kind
    for diagnostics and validates the configured length before using this check.
    """
    if len(data) != length:
        raise ProtocolError(f"expected {length} {what} bytes")
