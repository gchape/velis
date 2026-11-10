"""Compose independent presenters into one terminal frame."""

import curses

from .chrome import draw_context, draw_footer, draw_header
from .layout import MIN_COLS, MIN_ROWS, Layout
from .overlays import draw_overlay
from .panes import ValuePane, draw_activity, draw_keys
from .state import Mode, ViewState
from .surface import Surface
from .workspace import Workspace

__all__ = ["Mode", "Renderer", "ViewState"]


class Renderer:
    """Coordinate layout and drawing; input and network operations live elsewhere."""

    def __init__(
        self,
        scr: curses.window,
        workspace: Workspace,
        state: ViewState,
        *,
        host: str,
        port: int,
    ) -> None:
        self._surface = Surface(scr)
        self._ws = workspace
        self._state = state
        self._where = f"{host}:{port}"
        self._value = ValuePane(self._surface, state)

    def draw(self) -> None:
        """Choose a view and paint it within the current terminal dimensions."""
        surface, screen = self._surface, self._surface.screen
        screen.erase()
        height, width = screen.getmaxyx()
        if height < MIN_ROWS or width < MIN_COLS:
            surface.put(
                0, 0, f"Resize terminal to at least {MIN_COLS} x {MIN_ROWS}", width
            )
            surface.cursor(None)
        else:
            draw_header(surface, self._ws, self._where, width)
            self._content(height, width)
        screen.refresh()

    def _content(self, height: int, width: int) -> None:
        """Delegate each region to its presenter without sharing curses internals."""
        surface, ws, state = self._surface, self._ws, self._state
        if state.mode in {Mode.HELP, Mode.STATS}:
            draw_overlay(surface, ws, state, height, width)
            surface.cursor(None)
            return
        draw_context(surface, ws, state, width)
        layout = Layout.for_size(height, width)
        draw_keys(surface, layout.keys, ws, state)
        for row in range(layout.keys.y, layout.keys.y + layout.keys.rows):
            surface.put(row, layout.keys.width, "│", 1, surface.theme.dim)
        self._value.draw(
            layout.value,
            state.selected_entry(list(ws.model.entries.values())),
            read_only=ws.read_only,
        )
        draw_activity(surface, layout.activity, ws)
        draw_footer(surface, ws, state, height, width)
