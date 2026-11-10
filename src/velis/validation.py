"""Shared checks for positive runtime and resource limits."""


def require_positive_int(**settings: int) -> None:
    """Counts and byte budgets must be positive integers, excluding booleans."""
    for name, value in settings.items():
        if type(value) is not int or value <= 0:
            raise ValueError(f"invalid {name}: {value}")


def require_positive(**settings: int | float) -> None:
    """Require finite positive settings, naming the first invalid value."""
    for name, value in settings.items():
        if not 0 < value < float("inf"):
            raise ValueError(f"invalid {name}: {value}")
