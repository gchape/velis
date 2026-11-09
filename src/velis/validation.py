"""Shared checks for positive runtime and resource limits."""


def require_positive(**settings: int) -> None:
    """Raise ValueError for the first zero or negative setting, naming it."""
    for name, value in settings.items():
        if value <= 0:
            raise ValueError(f"invalid {name}: {value}")
