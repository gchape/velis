"""Shared argument checks for configuration objects."""


def require_positive(**settings: int) -> None:
    """Reject nonpositive settings, naming the argument that failed."""
    for name, value in settings.items():
        if value <= 0:
            raise ValueError(f"invalid {name}: {value}")
