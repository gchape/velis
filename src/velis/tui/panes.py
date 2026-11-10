"""Independent key, value and activity presenters."""

import time

from velis.branding import HEAD_ROWS, MASCOT, WORD

from .layout import Rect
from .models import MARKS, Entry, Status, ttl_text
from .state import ViewState
from .surface import Surface
from .text import clean, describe_value, pad, wrap_cells
from .workspace import Workspace


def draw_keys(surface: Surface, rect: Rect, ws: Workspace, state: ViewState) -> None:
    """Show selection and expiry without fetching or changing cache usage."""
    entries = state.visible_entries(list(ws.model.entries.values()))
    surface.heading(rect.y, rect.x, f" Keys · {len(entries)}", rect.width)
    state.keep_visible(entries, rect.rows - 1)
    if not entries:
        surface.put(
            rect.y + 2,
            rect.x + 1,
            "No matches" if state.query else "No keys yet",
            rect.width - 1,
        )
        return
    now = time.monotonic()
    for row, entry in enumerate(entries[state.top : state.top + rect.rows - 1]):
        selected = state.top + row == state.selected
        marker = ">" if selected else " "
        text = (
            f"{marker} {pad(clean(entry.key), rect.width - 11)} "
            f"{ttl_text(entry, now):>8}"
        )
        attr = surface.theme.selected if selected else 0
        if entry.status is Status.GONE and not selected:
            attr = surface.theme.dim
        surface.put(rect.y + 1 + row, rect.x, pad(text, rect.width), rect.width, attr)


def draw_activity(surface: Surface, rect: Rect, ws: Workspace) -> None:
    """Display recent events with textual severity as well as color."""
    surface.heading(rect.y, rect.x, " Activity", rect.width)
    if not ws.activity:
        surface.put(
            rect.y + 1,
            rect.x + 1,
            "Ready for your first command",
            rect.width - 2,
            surface.theme.dim,
        )
    for row, item in enumerate(list(ws.activity)[-(rect.rows - 1) :]):
        text = f"{item.when} {MARKS[item.kind]} {item.text}"
        surface.put(
            rect.y + 1 + row,
            rect.x + 1,
            text,
            rect.width - 2,
            surface.theme.activity[item.kind],
        )


class ValuePane:
    """Present cached metadata, wrapped values and an actionable empty state."""

    def __init__(self, surface: Surface, state: ViewState) -> None:
        self._surface = surface
        self._state = state

    def draw(self, rect: Rect, entry: Entry | None, *, read_only: bool) -> None:
        """Keep metadata compact so even a short terminal has preview space."""
        surface, state = self._surface, self._state
        surface.heading(rect.y, rect.x, " Value preview", rect.width)
        if entry is None:
            self._empty(rect, read_only)
            return
        if entry.key != state.shown:
            state.shown, state.scroll = entry.key, 0
        size = "not fetched" if entry.size is None else f"{entry.size:,} bytes"
        status = entry.status.name.lower()
        meta = f"{status} · {size} · left {ttl_text(entry, time.monotonic())}"
        surface.put(rect.y + 1, rect.x + 1, entry.key, rect.width - 2)
        surface.put(rect.y + 2, rect.x + 1, meta, rect.width - 2, surface.theme.dim)
        self._body(rect, self._lines(entry, rect.width - 2))

    def _lines(self, entry: Entry, width: int) -> list[str]:
        """Describe value state without making a network request."""
        if entry.status is Status.GONE:
            return ["Key no longer exists. Enter checks again."]
        if entry.size is None:
            return ["Press Enter to fetch this value."]
        if entry.size == 0:
            return ["(empty value)"]
        return describe_value(entry.preview, entry.size, width)

    def _body(self, rect: Rect, lines: list[str]) -> None:
        """Clamp scrolling after resize and show the visible line range."""
        state, surface = self._state, self._surface
        visible = max(1, rect.rows - 3)
        state.page = max(1, visible - 1)
        state.scroll = max(0, min(state.scroll, len(lines) - visible))
        for row, line in enumerate(lines[state.scroll : state.scroll + visible]):
            surface.put(rect.y + 3 + row, rect.x + 1, line, rect.width - 2)
        if len(lines) > visible:
            end = min(len(lines), state.scroll + visible)
            hint = f" {state.scroll + 1}-{end}/{len(lines)} "
            surface.put(
                rect.y,
                rect.x + rect.width - len(hint),
                hint,
                len(hint),
                surface.theme.dim,
            )

    def _empty(self, rect: Rect, read_only: bool) -> None:
        """Offer commands that work in the current permission mode."""
        lines = ["Browse keys: :list", "Read a key: :read name"]
        if not read_only:
            lines.append('Write a key: :write name "hello"')
        lines.extend(
            (
                "",
                "Keys you use appear here.",
                "Tab completes actions. ? shows help.",
            )
        )
        lines = [part for line in lines for part in wrap_cells(line, rect.width - 2)]
        top = rect.y + 1
        if rect.rows >= len(lines) + len(MASCOT) + 2 and rect.width >= 48:
            self._brand(top, rect.x + 1, rect.width - 2)
            top += len(MASCOT) + 1
        for row, line in enumerate(lines[: rect.y + rect.rows - top]):
            self._surface.put(top + row, rect.x + 1, line, rect.width - 2)

    def _brand(self, top: int, x: int, width: int) -> None:
        """Use the shared mascot only when it leaves room for the next action."""
        surface = self._surface
        word_x = x + max(map(len, MASCOT)) + 2
        for row, (art, word) in enumerate(zip(MASCOT, WORD, strict=True)):
            attr = (
                surface.theme.brand_head
                if row < HEAD_ROWS
                else surface.theme.brand_body
            )
            surface.put(top + row, x, art, width, attr)
            surface.put(top + row, word_x, word, x + width - word_x)
