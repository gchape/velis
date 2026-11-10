"""Terminal drawing adapter and accessible color attributes."""

import curses
import os
from contextlib import suppress

from .models import Kind
from .text import clean, fit, pad


class Theme:
    """Use terminal defaults, with readable monochrome and NO_COLOR fallbacks."""

    def __init__(self) -> None:
        self.title = curses.A_BOLD | curses.A_REVERSE
        self.pane = curses.A_BOLD
        self.dim = curses.A_DIM
        self.selected = curses.A_REVERSE
        self.brand_head = curses.A_BOLD
        self.brand_body = 0
        self.activity = dict.fromkeys(Kind, 0)
        self.activity[Kind.ERROR] = curses.A_BOLD
        if curses.has_colors() and not os.environ.get("NO_COLOR"):
            with suppress(curses.error):
                self._colors()

    def _colors(self) -> None:
        """Allocate semantic colors without changing the user's background."""
        try:
            curses.use_default_colors()
            background = -1
        except curses.error:
            background = curses.COLOR_BLACK
        colors = (
            curses.COLOR_GREEN,
            curses.COLOR_YELLOW,
            curses.COLOR_RED,
            curses.COLOR_CYAN,
        )
        kinds = (Kind.OK, Kind.WARN, Kind.ERROR, Kind.PUSH)
        for number, (kind, color) in enumerate(zip(kinds, colors, strict=True), 1):
            curses.init_pair(number, color, background)
            self.activity[kind] |= curses.color_pair(number)
        head, body = (141, 61) if curses.COLORS >= 256 else (5, 5)
        curses.init_pair(5, head, background)
        curses.init_pair(6, body, background)
        self.brand_head = curses.color_pair(5) | curses.A_BOLD
        self.brand_body = curses.color_pair(6)
        self.pane |= curses.color_pair(5)


class Surface:
    """Clip and escape terminal writes at the final output boundary."""

    def __init__(self, screen: curses.window) -> None:
        self.screen = screen
        self.theme = Theme()
        self._cursor_visible: bool | None = None

    def put(self, y: int, x: int, text: str, width: int, attr: int = 0) -> None:
        """Write inert text within the current screen, tolerating resize races."""
        height, columns = self.screen.getmaxyx()
        width = min(width, columns - x)
        if 0 <= y < height and 0 <= x < columns and width > 0:
            with suppress(curses.error):
                self.screen.addstr(y, x, fit(clean(text), width), attr)

    def heading(self, y: int, x: int, title: str, width: int) -> None:
        """Separate a pane with a quiet rule and a consistent title."""
        self.put(
            y,
            x,
            pad(title + " ", width),
            width,
            self.theme.pane | curses.A_UNDERLINE,
        )

    def cursor(self, position: tuple[int, int] | None) -> None:
        """Show the input caret only while editing on a usable screen."""
        if position is not None:
            with suppress(curses.error):
                self.screen.move(*position)
        visible = position is not None
        if visible != self._cursor_visible:
            with suppress(curses.error):
                curses.curs_set(int(visible))
            self._cursor_visible = visible
