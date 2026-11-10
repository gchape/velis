"""Pure geometry for the three-pane terminal workspace."""

from dataclasses import dataclass

MIN_ROWS, MIN_COLS = 14, 60


@dataclass(frozen=True, slots=True)
class Rect:
    """A rectangular pane in terminal cells, including its title row."""

    y: int
    x: int
    rows: int
    width: int


@dataclass(frozen=True, slots=True)
class Layout:
    """Reserve status and command rows while keeping value and activity visible."""

    keys: Rect
    value: Rect
    activity: Rect

    @classmethod
    def for_size(cls, height: int, width: int) -> Layout:
        """Allocate space for a terminal at least MIN_COLS by MIN_ROWS."""
        rows = height - 5
        key_width = min(36, max(21, width // 3))
        right = key_width + 2
        activity_rows = max(3, min(7, rows // 3))
        return cls(
            Rect(3, 0, rows, key_width),
            Rect(3, right, rows - activity_rows, width - right),
            Rect(3 + rows - activity_rows, right, activity_rows, width - right),
        )
