"""Shared argument checks for configuration objects."""


def require_positive(name: str, value: int) -> None:
    """Raise ValueError naming the setting when value is not positive."""
    if value <= 0:
        raise ValueError(f"invalid {name}: {value}")
