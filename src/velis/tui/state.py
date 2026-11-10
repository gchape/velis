"""Presentation state shared by input and rendering, independent of curses."""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum, auto

from .models import Entry


class Mode(Enum):
    """Active browse, command or informational screen."""

    BROWSE = auto()
    COMMAND = auto()
    FILTER = auto()
    HELP = auto()
    STATS = auto()


@dataclass(slots=True)
class ViewState:
    """Selection, scrolling and command draft shared by input and rendering."""

    mode: Mode = Mode.BROWSE
    query: str = ""
    line: str = ""
    cursor: int = 0
    selected: int = 0
    top: int = 0
    scroll: int = 0
    page: int = 10
    shown: str | None = None
    follow: str | None = None
    overlay_scroll: int = 0
    overlay_page: int = 10
    overlay_total: int = 0

    def selected_entry(self, entries: Sequence[Entry]) -> Entry | None:
        """Return the selected key, clamping after keys have been forgotten."""
        entries = self.visible_entries(entries)
        if not entries:
            return None
        return entries[max(0, min(self.selected, len(entries) - 1))]

    def visible_entries(self, entries: Sequence[Entry]) -> list[Entry]:
        """Filter local observations by key name without reading from the server."""
        query = self.query.casefold()
        return [entry for entry in entries if query in entry.key.casefold()]

    def keep_visible(self, entries: list[Entry], visible: int) -> None:
        """Follow a submitted key and keep the selection inside the key pane."""
        if self.follow is not None:
            for index, entry in enumerate(entries):
                if entry.key == self.follow:
                    self.selected = index
                    break
            self.follow = None
        self.selected = max(0, min(self.selected, len(entries) - 1))
        self.top = max(self.selected - visible + 1, min(self.top, self.selected))
        self.top = max(0, min(self.top, max(0, len(entries) - visible)))
